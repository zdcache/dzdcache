package io.github.dzdcache.server.schema;

import io.github.dzdcache.json.Definitions;
import io.github.dzdcache.json.DimensionDefinition;
import io.github.dzdcache.json.SchemaDefinition;
import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import javax.tools.DiagnosticCollector;
import javax.tools.FileObject;
import javax.tools.ForwardingJavaFileManager;
import javax.tools.JavaFileObject;
import javax.tools.SimpleJavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ObjectNode;

/** Compiles a deliberately bounded JSON Schema subset into isolated, JDK-only classes. */
public final class RuntimeSchemaCompiler {
    static final int MAX_DEPTH = Definitions.MAX_DEPTH;
    static final int MAX_PROPERTIES = 512;
    static final int MAX_CLASSES = 128;
    static final int MAX_SCHEMA_BYTES = 262_144;
    private static final String ROOT_NAME = "io.github.dzdcache.generated.Root";
    private static final Set<String> METADATA = Set.of("$schema", "$id", "$comment", "title", "description", "default", "examples");
    private final ObjectMapper mapper;

    public RuntimeSchemaCompiler(ObjectMapper mapper) {
        this.mapper = java.util.Objects.requireNonNull(mapper);
    }

    public CompiledSchema compile(SchemaDefinition definition) {
        validateDefinition(definition);
        if (definition.schema() == null || definition.schema().toString().getBytes(StandardCharsets.UTF_8).length > MAX_SCHEMA_BYTES) {
            throw invalid("Schema is missing or exceeds " + MAX_SCHEMA_BYTES + " bytes");
        }
        ParseState state = new ParseState();
        Node root = parse(definition.schema(), "schema", 0, state);
        if (root.kind != Kind.OBJECT || root.nullable) {
            throw invalid("The root schema must describe a non-null object");
        }
        Node identity = resolve(root, definition.identity());
        if (segments(definition.identity()).contains("*") || !identity.scalar() || identity.nullable) {
            throw invalid("Identity must select a non-null scalar without array traversal");
        }
        Set<String> dimensionNames = new HashSet<>();
        for (DimensionDefinition dimension : definition.dimensions()) {
            validateName(dimensionNames, dimension);
            if (dimension.type() != null || dimension.parts().stream().anyMatch(part -> part.type() != null))
                throw invalid("A dimension type is only declared without a schema; the schema decides the type");
            if (dimension.index().equalsIgnoreCase("compound")) validateCompound(root, dimension);
            else {
                if (!dimension.parts().isEmpty()) throw invalid("Only a compound dimension declares parts");
                validatePart(root, dimension, dimension.path() != null && dimension.path().contains("*"));
            }
        }
        String source = generateSource(root, state.objects);
        ClassLoader loader = compileSource(source);
        try {
            for (Node object : state.objects) {
                object.javaClass = loader.loadClass(object == root ? ROOT_NAME : ROOT_NAME + "$" + object.className);
                for (Property property : object.properties.values()) {
                    property.field = object.javaClass.getField(property.fieldName);
                }
                object.presenceFields = new java.lang.reflect.Field[words(object)];
                for (int i = 0; i < object.presenceFields.length; i++) object.presenceFields[i] = object.javaClass.getField("p" + i);
                object.extraField = object.javaClass.getField("extra");
                object.constructor = object.javaClass.getConstructor();
            }
            @SuppressWarnings("unchecked") Class<Object> rootClass = (Class<Object>) root.javaClass;
            return new CompiledSchema(mapper, root, rootClass, fingerprint(definition));
        } catch (ReflectiveOperationException exception) {
            throw new IllegalStateException("Generated schema class could not be loaded", exception);
        }
    }

    private static void validateCompound(Node root, DimensionDefinition dimension) {
        List<DimensionDefinition> parts = dimension.parts();
        if (dimension.path() != null) throw invalid("A compound dimension declares parts instead of a path");
        if (parts.size() < 2 || parts.size() > 8) throw invalid("A compound dimension needs 2 to 8 parts");
        String element = sharedElement(parts);
        for (int i = 0; i < parts.size(); i++) {
            DimensionDefinition part = parts.get(i);
            if (part.index() == null || part.index().equalsIgnoreCase("compound") || !part.parts().isEmpty())
                throw invalid("Compound parts must be exact, prefix or range");
            if (i < parts.size() - 1 && !part.index().equalsIgnoreCase("exact"))
                throw invalid("Only the last compound part can be prefix or range");
            String within = element == null ? part.path() : part.path().substring(element.length());
            validatePart(root, part, element == null && within.contains("*"));
        }
    }

    private static void validatePart(Node root, DimensionDefinition dimension, boolean traversesArrays) {
        Node leaf = resolve(root, dimension.path());
        if (!leaf.scalar()) throw invalid("Dimension " + dimension.path() + " must select scalar values");
        String index = dimension.index().toLowerCase(Locale.ROOT);
        if (!Set.of("exact", "prefix", "range").contains(index)) {
            throw invalid("Unsupported index: " + dimension.index());
        }
        if (index.equals("prefix") && leaf.kind != Kind.STRING) {
            throw invalid("A prefix dimension must select strings");
        }
        if (index.equals("range") && (traversesArrays || leaf.kind == Kind.BOOLEAN)) {
            throw invalid("A range dimension must select one string or numeric value");
        }
    }

    static void validateName(Set<String> names, DimensionDefinition dimension) { Definitions.validateName(names, dimension); }

    public static String sharedElement(List<DimensionDefinition> parts) {
        String shared = null;
        for (DimensionDefinition part : parts) {
            if (part == null || part.path() == null) throw invalid("Compound parts need a path");
            int star = part.path().indexOf("/*");
            if (star < 0) return null;
            String element = part.path().substring(0, star + 2);
            if (part.path().indexOf("/*", star + 2) >= 0) return null;
            if (shared == null) shared = element;
            else if (!shared.equals(element)) return null;
        }
        return shared;
    }

    static void validateDefinition(SchemaDefinition definition) { Definitions.validateDefinition(definition); }

    private Node parse(JsonNode json, String location, int depth, ParseState state) {
        if (depth > MAX_DEPTH) throw invalid("Schema nesting exceeds " + MAX_DEPTH);
        if (json == null || !json.isObject()) throw invalid(location + " must be a schema object");
        JsonNode type = json.get("type");
        boolean nullable = false;
        String name = null;
        if (type != null && type.isTextual()) {
            name = type.asText();
        } else if (type != null && type.isArray() && type.size() == 2) {
            for (JsonNode member : type) {
                if (!member.isTextual()) throw invalid(location + ": type entries must be strings");
                if (member.asText().equals("null")) {
                    if (nullable) throw invalid(location + ": duplicate null type");
                    nullable = true;
                } else {
                    if (name != null) throw invalid(location + ": only [type, null] unions are supported");
                    name = member.asText();
                }
            }
            if (!nullable) throw invalid(location + ": only [type, null] unions are supported");
        }
        if (name == null) throw invalid(location + ": an explicit supported type is required");
        Kind kind;
        try { kind = Kind.valueOf(name.toUpperCase(Locale.ROOT)); }
        catch (IllegalArgumentException exception) { throw invalid(location + ": unsupported type " + name); }
        Node node = new Node(kind, nullable);
        Set<String> allowed = new HashSet<>(METADATA);
        allowed.add("type");
        allowed.add("enum");
        switch (kind) {
            case OBJECT -> allowed.addAll(Set.of("properties", "required", "additionalProperties"));
            case ARRAY -> allowed.add("items");
            default -> { }
        }
        for (Map.Entry<String, JsonNode> entry : json.properties()) {
            if (!allowed.contains(entry.getKey())) throw invalid(location + ": unsupported keyword " + entry.getKey());
        }
        if (kind == Kind.OBJECT) {
            if (state.objects.size() >= MAX_CLASSES) throw invalid("Schema exceeds " + MAX_CLASSES + " generated classes");
            node.className = state.objects.isEmpty() ? "Root" : "Object" + state.objects.size();
            state.objects.add(node);
            JsonNode additional = json.get("additionalProperties");
            if (additional != null && !additional.isBoolean()) throw invalid(location + ": additionalProperties must be boolean");
            node.additionalProperties = additional == null || additional.asBoolean();
            JsonNode properties = json.get("properties");
            if (properties != null && !properties.isObject()) throw invalid(location + ": properties must be an object");
            if (properties != null) {
                // Sorting makes generated class structure independent of input property order.
                Map<String, JsonNode> sorted = new TreeMap<>();
                properties.properties().forEach(entry -> sorted.put(entry.getKey(), entry.getValue()));
                for (Map.Entry<String, JsonNode> property : sorted.entrySet()) {
                    if (++state.properties > MAX_PROPERTIES) throw invalid("Schema exceeds " + MAX_PROPERTIES + " properties");
                    if (property.getKey().length() > 256) throw invalid("Property names may not exceed 256 characters");
                    Node child = parse(property.getValue(), location + "/" + property.getKey(), depth + 1, state);
                    node.properties.put(property.getKey(), new Property("f" + node.properties.size(), node.properties.size(), node, child));
                }
            }
            JsonNode required = json.get("required");
            if (required != null) {
                if (!required.isArray()) throw invalid(location + ": required must be an array");
                for (JsonNode property : required) {
                    if (!property.isTextual() || !node.properties.containsKey(property.asText()) || !node.required.add(property.asText())) {
                        throw invalid(location + ": required must name unique declared properties");
                    }
                }
            }
        } else if (kind == Kind.ARRAY) {
            node.items = parse(json.get("items"), location + "/items", depth + 1, state);
        }
        JsonNode values = json.get("enum");
        if (values != null) {
            if (!node.scalar() || !values.isArray() || values.isEmpty() || values.size() > 256) {
                throw invalid(location + ": enum requires 1-256 scalar values");
            }
            node.enumValues = new HashMap<>();
            for (JsonNode value : values) {
                Object scalar = CompiledSchema.scalar(node, value, location, false);
                if (node.enumValues.containsKey(scalar)) throw invalid(location + ": duplicate enum value");
                node.enumValues.put(scalar, scalar);
            }
        }
        return node;
    }

    static Node resolve(Node root, String path) {
        Node current = root;
        for (String segment : segments(path)) {
            if (current.kind == Kind.ARRAY) {
                if (!segment.equals("*")) throw invalid("Array path must use '*': " + path);
                current = current.items;
            } else if (current.kind == Kind.OBJECT) {
                Property property = current.properties.get(segment);
                if (property == null) throw invalid("Path references an undeclared property: " + path);
                current = property.node;
            } else throw invalid("Path traverses a scalar: " + path);
        }
        return current;
    }

    static List<String> segments(String path) { return Definitions.segments(path); }

    private static String generateSource(Node root, List<Node> objects) {
        StringBuilder source = new StringBuilder("package io.github.dzdcache.generated;\npublic final class Root {\n");
        appendFields(source, root);
        for (Node node : objects) {
            if (node == root) continue;
            source.append("public static final class ").append(node.className).append(" {\n");
            appendFields(source, node);
            source.append("}\n");
        }
        return source.append("}\n").toString();
    }

    private static void appendFields(StringBuilder source, Node node) {
        for (int i = 0; i < words(node); i++) source.append("public long p").append(i).append(";\n");
        source.append("public java.util.Map<String,Object> extra;\n");
        for (Property property : node.properties.values()) {
            source.append("public ").append(fieldType(property.node)).append(' ').append(property.fieldName).append(";\n");
        }
    }

    static int words(Node node) { return Math.max(1, (node.properties.size() + 63) / 64); }

    private static String fieldType(Node node) {
        if (!node.nullable && node.kind == Kind.INTEGER) return "long";
        if (!node.nullable && node.kind == Kind.BOOLEAN) return "boolean";
        return javaType(node);
    }

    private static String javaType(Node node) {
        return switch (node.kind) {
            case STRING -> "java.lang.String";
            case INTEGER -> "java.lang.Long";
            case NUMBER -> "java.math.BigDecimal";
            case BOOLEAN -> "java.lang.Boolean";
            case OBJECT -> node.className;
            case ARRAY -> "java.util.List<" + javaType(node.items) + ">";
        };
    }

    private static ClassLoader compileSource(String source) {
        var compiler = ToolProvider.getSystemJavaCompiler();
        if (compiler == null) throw new IllegalStateException("Schema compilation requires a full JDK, including jdk.compiler");
        DiagnosticCollector<JavaFileObject> diagnostics = new DiagnosticCollector<>();
        Map<String, ByteCode> output = new HashMap<>();
        try (StandardJavaFileManager standard = compiler.getStandardFileManager(diagnostics, Locale.ROOT, StandardCharsets.UTF_8);
             var manager = new ForwardingJavaFileManager<StandardJavaFileManager>(standard) {
                 @Override public JavaFileObject getJavaFileForOutput(Location location, String name,
                         JavaFileObject.Kind kind, FileObject sibling) {
                     ByteCode bytes = new ByteCode(name);
                     output.put(name, bytes);
                     return bytes;
                 }
             }) {
            JavaFileObject input = new SimpleJavaFileObject(URI.create("string:///io/github/dzdcache/generated/Root.java"), JavaFileObject.Kind.SOURCE) {
                @Override public CharSequence getCharContent(boolean ignoreEncodingErrors) { return source; }
            };
            // Generated source uses only JDK types. Annotation processors and the application classpath are excluded.
            boolean successful = Boolean.TRUE.equals(compiler.getTask(null, manager, diagnostics,
                    List.of("--release", "21", "-proc:none", "-classpath", "", "-g:none"), null, List.of(input)).call());
            if (!successful) throw new IllegalStateException("Generated schema compilation failed: " + diagnostics.getDiagnostics());
        } catch (java.io.IOException exception) {
            throw new IllegalStateException("Cannot compile schema", exception);
        }
        Map<String, byte[]> classes = new HashMap<>();
        output.forEach((name, bytes) -> classes.put(name, bytes.output.toByteArray()));
        return new ClassLoader(ClassLoader.getPlatformClassLoader()) {
            @Override protected Class<?> findClass(String name) throws ClassNotFoundException {
                byte[] bytes = classes.get(name);
                if (bytes == null) throw new ClassNotFoundException(name);
                return defineClass(name, bytes, 0, bytes.length);
            }
        };
    }

    public String fingerprint(SchemaDefinition definition) { return Definitions.fingerprint(definition, mapper); }

    static IllegalArgumentException invalid(String message) { return new IllegalArgumentException(message); }
    enum Kind { OBJECT, ARRAY, STRING, INTEGER, NUMBER, BOOLEAN }
    static final class ParseState {
        int properties;
        final List<Node> objects = new ArrayList<>();
    }
    static final class Node {
        final Kind kind;
        final boolean nullable;
        final Map<String, Property> properties = new LinkedHashMap<>();
        final Set<String> required = new HashSet<>();
        boolean additionalProperties;
        Node items;
        String className;
        Class<?> javaClass;
        java.lang.reflect.Field[] presenceFields;
        java.lang.reflect.Field extraField;
        java.lang.reflect.Constructor<?> constructor;
        Map<Object, Object> enumValues;
        Node(Kind kind, boolean nullable) { this.kind = kind; this.nullable = nullable; }
        boolean scalar() { return kind != Kind.OBJECT && kind != Kind.ARRAY; }
    }
    static final class Property {
        final String fieldName;
        final int bit;
        final Node owner;
        final Node node;
        java.lang.reflect.Field field;
        Property(String fieldName, int bit, Node owner, Node node) {
            this.fieldName = fieldName; this.bit = bit; this.owner = owner; this.node = node;
        }
    }
    private static final class ByteCode extends SimpleJavaFileObject {
        final ByteArrayOutputStream output = new ByteArrayOutputStream();
        ByteCode(String name) { super(URI.create("bytes:///" + name.replace('.', '/') + ".class"), JavaFileObject.Kind.CLASS); }
        @Override public OutputStream openOutputStream() { return output; }
    }
}
