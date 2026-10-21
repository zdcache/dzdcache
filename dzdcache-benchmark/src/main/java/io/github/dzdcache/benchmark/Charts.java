package io.github.dzdcache.benchmark;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

final class Charts {
    record Bar(String subject, double value) { }
    record Group(String label, List<Bar> bars) { }

    private static final String[] LIGHT = {"#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#6250d6", "#e34948"};
    private static final String[] DARK = {"#3987e5", "#d95926", "#199e70", "#c98500", "#d55181", "#008300", "#9085e9", "#e66767"};
    private static final Map<String, Integer> SLOTS = Map.of("dzdcache-embedded", 0, "dzdcache-server", 0, "hazelcast", 1, "ignite", 2,
        "infinispan", 3, "redis-stack", 4);

    static final int WIDTH = 720;
    private static final int TIP = 64;
    private static final int BAR = 14;
    private static final int GAP = 2;
    private static final int GROUP_GAP = 18;
    private static final int AXIS = 44;

    private Charts() { }

    static int slot(String subject, List<String> subjects) {
        Integer known = SLOTS.get(subject);
        if (known != null) return known;
        int unknown = 0;
        for (String other : subjects) {
            if (other.equals(subject)) break;
            if (!SLOTS.containsKey(other)) unknown++;
        }
        return Math.min(5 + unknown, LIGHT.length - 1);
    }

    static String bars(String title, String axis, List<String> subjects, List<Group> groups, boolean log) {
        double min = Double.MAX_VALUE, max = 0;
        for (Group group : groups) for (Bar bar : group.bars()) { min = Math.min(min, bar.value()); max = Math.max(max, bar.value()); }
        if (max <= 0) return null;
        double low, high;
        List<Double> ticks = new ArrayList<>();
        if (log) {
            low = Math.pow(10, Math.floor(Math.log10(Math.max(min, 1) / 2)));
            high = Math.pow(10, Math.ceil(Math.log10(max)));
            if (high <= low) high = low * 10;
            for (double tick = low; tick <= high * 1.0001; tick *= 10) ticks.add(tick);
        } else {
            low = 0;
            double step = niceStep(max / 4);
            high = Math.ceil(max / step) * step;
            for (double tick = 0; tick <= high + step / 2; tick += step) ticks.add(tick);
        }
        int labels = groups.stream().mapToInt(group -> (int) Math.ceil(group.label().length() * 7.2) + 20).max().orElse(0);
        double plotLeft = labels, plotRight = WIDTH - TIP;
        double lowEdge = log ? Math.log10(low) : low, span = (log ? Math.log10(high) : high) - lowEdge;
        java.util.function.DoubleUnaryOperator x = value -> plotLeft + ((log ? Math.log10(Math.max(value, low)) : value) - lowEdge) / span * (plotRight - plotLeft);

        boolean legend = groups.stream().anyMatch(group -> group.bars().size() > 1);
        int top = legend ? 40 : 16;
        int plotHeight = 0;
        for (Group group : groups) plotHeight += group.bars().size() * (BAR + GAP) - GAP + GROUP_GAP;
        plotHeight -= GROUP_GAP;
        int height = top + plotHeight + AXIS;

        StringBuilder svg = new StringBuilder();
        svg.append("<svg xmlns=\"http://www.w3.org/2000/svg\" width=\"").append(WIDTH).append("\" height=\"").append(height)
            .append("\" viewBox=\"0 0 ").append(WIDTH).append(' ').append(height).append("\" role=\"img\" aria-labelledby=\"title\">\n");
        svg.append("<title id=\"title\">").append(escape(title)).append("</title>\n");
        svg.append("<style>\n").append(style()).append("</style>\n");
        svg.append("<rect class=\"surface\" width=\"100%\" height=\"100%\" rx=\"8\"/>\n");

        double legendX = plotLeft;
        for (String subject : legend ? subjects : List.<String>of()) {
            if (groups.stream().noneMatch(group -> group.bars().stream().anyMatch(bar -> bar.subject().equals(subject)))) continue;
            svg.append(String.format(Locale.ROOT, "<rect class=\"s%d\" x=\"%.1f\" y=\"14\" width=\"10\" height=\"10\" rx=\"2\"/>", slot(subject, subjects), legendX));
            svg.append(String.format(Locale.ROOT, "<text class=\"secondary\" x=\"%.1f\" y=\"23\">%s</text>\n", legendX + 15, escape(subject)));
            legendX += 15 + subject.length() * 6.6 + 20;
        }

        for (double tick : ticks) {
            double at = x.applyAsDouble(tick);
            svg.append(String.format(Locale.ROOT, "<line class=\"grid\" x1=\"%.1f\" y1=\"%d\" x2=\"%.1f\" y2=\"%d\"/>", at, top - 6, at, top + plotHeight));
            svg.append(String.format(Locale.ROOT, "<text class=\"muted tick\" x=\"%.1f\" y=\"%d\" text-anchor=\"middle\">%s</text>\n",
                at, top + plotHeight + 16, log ? compact(tick) : String.format(Locale.ROOT, "%,.0f", tick)));
        }
        svg.append(String.format(Locale.ROOT, "<line class=\"baseline\" x1=\"%.1f\" y1=\"%d\" x2=\"%.1f\" y2=\"%d\"/>\n", plotLeft, top - 6, plotLeft, top + plotHeight));
        svg.append(String.format(Locale.ROOT, "<text class=\"muted\" x=\"%.1f\" y=\"%d\" text-anchor=\"middle\">%s</text>\n",
            (plotLeft + plotRight) / 2, top + plotHeight + 36, escape(axis)));

        int y = top;
        for (Group group : groups) {
            int groupHeight = group.bars().size() * (BAR + GAP) - GAP;
            svg.append(String.format(Locale.ROOT, "<text class=\"primary\" x=\"%d\" y=\"%.1f\" text-anchor=\"end\">%s</text>\n",
                labels - 12, y + groupHeight / 2.0 + 4, escape(group.label())));
            for (Bar bar : group.bars()) {
                double end = Math.max(x.applyAsDouble(bar.value()), plotLeft + 1);
                svg.append("<g><title>").append(escape(bar.subject() + ", " + group.label() + ": " + String.format(Locale.ROOT, "%,.0f", bar.value())))
                    .append("</title>");
                svg.append("<path class=\"s").append(slot(bar.subject(), subjects)).append("\" d=\"").append(bar(plotLeft, y, end - plotLeft)).append("\"/>");
                svg.append(String.format(Locale.ROOT, "<text class=\"secondary value\" x=\"%.1f\" y=\"%d\">%s</text></g>\n", end + 6, y + BAR - 3, log ? compact(bar.value()) : String.format(Locale.ROOT, "%,.0f", bar.value())));
                y += BAR + GAP;
            }
            y += GROUP_GAP - GAP;
        }
        return svg.append("</svg>\n").toString();
    }

    private static String bar(double left, double top, double width) {
        double radius = Math.min(4, width);
        return String.format(Locale.ROOT, "M%.1f %.1fH%.1fA%.1f %.1f 0 0 1 %.1f %.1fV%.1fA%.1f %.1f 0 0 1 %.1f %.1fH%.1fZ",
            left, top, left + width - radius, radius, radius, left + width, top + radius, top + BAR - radius,
            radius, radius, left + width - radius, top + BAR, left);
    }

    private static String style() {
        StringBuilder css = new StringBuilder();
        css.append("text{font-family:system-ui,-apple-system,\"Segoe UI\",sans-serif;font-size:12px}\n");
        css.append(".tick,.value{font-size:11px;font-variant-numeric:tabular-nums}\n");
        css.append(".grid{stroke-width:1}.baseline{stroke-width:1}\n");
        css.append(theme(LIGHT, "#fcfcfb", "#0b0b0b", "#52514e", "#898781", "#e1e0d9", "#c3c2b7"));
        css.append("@media (prefers-color-scheme: dark){\n")
            .append(theme(DARK, "#1a1a19", "#f0efec", "#c3c2b7", "#898781", "#2c2c2a", "#383835")).append("}\n");
        return css.toString();
    }

    private static String theme(String[] series, String surface, String primary, String secondary, String muted, String grid, String baseline) {
        StringBuilder css = new StringBuilder();
        css.append(".surface{fill:").append(surface).append("}.primary{fill:").append(primary).append("}.secondary{fill:").append(secondary)
            .append("}.muted{fill:").append(muted).append("}.grid{stroke:").append(grid).append("}.baseline{stroke:").append(baseline).append("}\n");
        for (int i = 0; i < series.length; i++) css.append(".s").append(i).append("{fill:").append(series[i]).append('}');
        return css.append('\n').toString();
    }

    static String compact(double value) {
        if (value >= 1e9) return trim(value / 1e9) + "G";
        if (value >= 1e6) return trim(value / 1e6) + "M";
        if (value >= 1e3) return trim(value / 1e3) + "K";
        return String.format(Locale.ROOT, "%.0f", value);
    }

    private static String trim(double value) {
        return value >= 100 ? String.format(Locale.ROOT, "%.0f", value) : String.format(Locale.ROOT, "%.1f", value).replace(".0", "");
    }

    private static double niceStep(double rough) {
        double magnitude = Math.pow(10, Math.floor(Math.log10(rough)));
        for (double nice : new double[] {1, 2, 2.5, 5, 10}) if (nice * magnitude >= rough) return nice * magnitude;
        return 10 * magnitude;
    }

    private static String escape(String text) { return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;"); }
}
