import tempfile
import unittest
from pathlib import Path

import pages

ROOT = Path(__file__).resolve().parents[2]


def site(files):
    directory = Path(tempfile.mkdtemp())
    for name, text in files.items():
        (directory / name).parent.mkdir(parents=True, exist_ok=True)
        (directory / name).write_text(text, encoding="utf-8")
    return pages.build(directory, "o/r")


class PagesTest(unittest.TestCase):
    def test_the_repository_builds_every_page_it_has(self):
        built = pages.build(ROOT, "zdcache/dzdcache")
        self.assertEqual(["", "getting-started", "configuration", "benchmark", "changelog"], list(built))
        self.assertIn("source: docs/CONFIGURATION.md\n", built["configuration"])

    def test_links_between_published_files_point_at_their_pages(self):
        built = site({"README.md": "[start](docs/GETTING_STARTED.md) [log](CHANGELOG.md)",
                      "CHANGELOG.md": "Log.",
                      "docs/GETTING_STARTED.md": "[config](CONFIGURATION.md#reads) [home](../README.md)",
                      "docs/CONFIGURATION.md": "[start](GETTING_STARTED.md#a-learner)"})
        self.assertIn("[start](getting-started/) [log](changelog/)", built[""])
        self.assertIn("[config](../configuration/#reads) [home](../)", built["getting-started"])
        self.assertIn("[start](../getting-started/#a-learner)", built["configuration"])

    def test_other_files_link_to_the_repository_relative_to_the_file_that_links_them(self):
        built = site({"README.md": "[api](openapi.yaml)",
                      "docs/GETTING_STARTED.md": "[k8s](../k8s/dzdcache.yaml) [tls](../dzdcache-spring-boot-starter/README.md#cluster-tls) [c](CONSISTENCY.md)"})
        self.assertIn("[api](https://github.com/o/r/blob/main/openapi.yaml)", built[""])
        self.assertIn("[k8s](https://github.com/o/r/blob/main/k8s/dzdcache.yaml)", built["getting-started"])
        self.assertIn("[tls](https://github.com/o/r/blob/main/dzdcache-spring-boot-starter/README.md#cluster-tls)", built["getting-started"])
        self.assertIn("[c](https://github.com/o/r/blob/main/docs/CONSISTENCY.md)", built["getting-started"])

    def test_anchors_and_absolute_links_stay_as_they_are(self):
        built = site({"README.md": "[a](#here) [b](https://example.org/x) [c](mailto:a@b.c)"})
        self.assertIn("[a](#here) [b](https://example.org/x) [c](mailto:a@b.c)", built[""])

    def test_benchmark_charts_are_served_beside_the_benchmark_page(self):
        built = site({"README.md": "Home.", "docs/BENCHMARK.md": "![chart](benchmark/memory.svg)"})
        self.assertIn("![chart](memory.svg)", built["benchmark"])

    def test_missing_files_have_no_page_and_links_to_them_go_to_the_repository(self):
        built = site({"README.md": "[report](docs/BENCHMARK.md)"})
        self.assertEqual([""], list(built))
        self.assertIn("[report](https://github.com/o/r/blob/main/docs/BENCHMARK.md)", built[""])


if __name__ == "__main__":
    unittest.main()
