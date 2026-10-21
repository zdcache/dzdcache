package io.github.dzdcache.cache;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import tools.jackson.databind.JsonNode;

public sealed interface Filter {
    int MAX_CONDITIONS = 64;

    enum Op { EQ, NE, LT, LTE, GT, GTE, IN, PREFIX, EXISTS }

    record All(List<Filter> filters) implements Filter { }
    record Any(List<Filter> filters) implements Filter { }
    record Not(Filter filter) implements Filter { }
    record Condition(String path, Op op, JsonNode value, String type) implements Filter { }

    interface Paths {
        Function<Object, List<Object>> values(String path, String type);
        Object literal(String path, JsonNode value, String type);
    }

    static Filter parse(JsonNode json) {
        int[] conditions = { 0 };
        return parse(json, conditions, 0);
    }

    private static Filter parse(JsonNode json, int[] conditions, int depth) {
        if (json == null || !json.isObject()) throw new IllegalArgumentException("A filter is an object");
        if (depth > 8) throw new IllegalArgumentException("A filter nests at most 8 levels");
        if (json.has("all") || json.has("any")) {
            String key = json.has("all") ? "all" : "any";
            JsonNode items = json.get(key);
            if (json.size() != 1 || !items.isArray() || items.isEmpty()) throw new IllegalArgumentException("'" + key + "' takes a non-empty array of filters and nothing else");
            List<Filter> filters = new ArrayList<>();
            for (JsonNode item : items) filters.add(parse(item, conditions, depth + 1));
            return key.equals("all") ? new All(List.copyOf(filters)) : new Any(List.copyOf(filters));
        }
        if (json.has("not")) {
            if (json.size() != 1) throw new IllegalArgumentException("'not' takes one filter and nothing else");
            return new Not(parse(json.get("not"), conditions, depth + 1));
        }
        if (++conditions[0] > MAX_CONDITIONS) throw new IllegalArgumentException("A filter has at most " + MAX_CONDITIONS + " conditions");
        JsonNode path = json.get("path");
        JsonNode op = json.get("op");
        if (path == null || !path.isString() || op == null || !op.isString())
            throw new IllegalArgumentException("A filter condition needs a 'path' and an 'op'");
        Op operator;
        try { operator = Op.valueOf(op.asString().toUpperCase(Locale.ROOT)); }
        catch (IllegalArgumentException e) { throw new IllegalArgumentException("Unknown filter op '" + op.asString() + "'"); }
        JsonNode value = json.get("value");
        if (operator == Op.EXISTS ? value != null : value == null || value.isNull())
            throw new IllegalArgumentException(operator == Op.EXISTS ? "'exists' takes no value" : "'" + op.asString() + "' needs a value");
        if (operator == Op.IN && (!value.isArray() || value.isEmpty() || value.size() > 256))
            throw new IllegalArgumentException("'in' takes an array of 1 to 256 values");
        if (operator != Op.IN && value != null && !value.isValueNode())
            throw new IllegalArgumentException("'" + op.asString() + "' takes a single value");
        JsonNode type = json.get("type");
        return new Condition(path.asString(), operator, value, type == null ? null : type.asString());
    }

    default Predicate<Object> compile(Paths paths) {
        return switch (this) {
            case All all -> {
                List<Predicate<Object>> parts = all.filters().stream().map(filter -> filter.compile(paths)).toList();
                yield root -> { for (Predicate<Object> part : parts) if (!part.test(root)) return false; return true; };
            }
            case Any any -> {
                List<Predicate<Object>> parts = any.filters().stream().map(filter -> filter.compile(paths)).toList();
                yield root -> { for (Predicate<Object> part : parts) if (part.test(root)) return true; return false; };
            }
            case Not not -> not.filter().compile(paths).negate();
            case Condition condition -> condition(condition, paths);
        };
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Predicate<Object> condition(Condition condition, Paths paths) {
        Function<Object, List<Object>> values = paths.values(condition.path(), condition.type());
        if (condition.op() == Op.EXISTS) return root -> !values.apply(root).isEmpty();
        if (condition.op() == Op.IN) {
            Set<Object> wanted = new HashSet<>();
            for (JsonNode item : condition.value()) wanted.add(normal(paths.literal(condition.path(), item, condition.type())));
            return root -> { for (Object value : values.apply(root)) if (wanted.contains(normal(value))) return true; return false; };
        }
        Object literal = normal(paths.literal(condition.path(), condition.value(), condition.type()));
        if (condition.op() == Op.PREFIX) {
            if (!(literal instanceof String prefix)) throw new IllegalArgumentException("'prefix' compares strings: " + condition.path());
            return root -> { for (Object value : values.apply(root)) if (value instanceof String text && text.startsWith(prefix)) return true; return false; };
        }
        if (condition.op() == Op.EQ) return root -> { for (Object value : values.apply(root)) if (literal.equals(normal(value))) return true; return false; };
        if (condition.op() == Op.NE) return root -> { for (Object value : values.apply(root)) if (literal.equals(normal(value))) return false; return true; };
        if (!(literal instanceof Comparable comparable) || literal instanceof Boolean)
            throw new IllegalArgumentException("'" + condition.op().name().toLowerCase(Locale.ROOT) + "' needs a comparable value: " + condition.path());
        return root -> {
            for (Object value : values.apply(root)) {
                Object normal = normal(value);
                if (normal == null || normal.getClass() != comparable.getClass()) continue;
                int order = ((Comparable) normal).compareTo(comparable);
                boolean matches = switch (condition.op()) {
                    case LT -> order < 0;
                    case LTE -> order <= 0;
                    case GT -> order > 0;
                    case GTE -> order >= 0;
                    default -> false;
                };
                if (matches) return true;
            }
            return false;
        };
    }

    private static Object normal(Object value) {
        return switch (value) {
            case null -> null;
            case Long number -> new BigDecimal(number);
            case Integer number -> new BigDecimal(number);
            case Short number -> new BigDecimal(number);
            case BigDecimal number -> number.stripTrailingZeros();
            case Double number -> BigDecimal.valueOf(number).stripTrailingZeros();
            case Float number -> BigDecimal.valueOf(number).stripTrailingZeros();
            default -> value;
        };
    }
}
