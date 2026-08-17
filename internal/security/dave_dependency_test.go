package security

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDaveBuildContractIsPinnedAcrossSourceCIReleaseAndDocker(t *testing.T) {
	root := filepath.Join("..", "..")

	read := func(path ...string) string {
		t.Helper()
		payload, err := os.ReadFile(filepath.Join(append([]string{root}, path...)...))
		if err != nil {
			t.Fatalf("read %s: %v", filepath.Join(path...), err)
		}
		return string(payload)
	}

	goMod := read("go.mod")
	for _, marker := range []string{
		"require github.com/cartridge-gg/discordgo v0.29.1-dave.25",
		"replace github.com/cartridge-gg/discordgo => ./third_party/discordgo",
	} {
		if !strings.Contains(goMod, marker) {
			t.Fatalf("go.mod is missing pinned DAVE dependency marker %q", marker)
		}
	}

	discordgoGoMod := read("third_party", "discordgo", "go.mod")
	if !strings.Contains(discordgoGoMod, "module github.com/cartridge-gg/discordgo") {
		t.Fatal("third_party/discordgo is missing the pinned local discordgo source")
	}

	gitmodules := read(".gitmodules")
	for _, marker := range []string{
		"path = third_party/discordgo/dave/libdave",
		"url = https://github.com/discord/libdave.git",
	} {
		if !strings.Contains(gitmodules, marker) {
			t.Fatalf(".gitmodules is missing DAVE source marker %q", marker)
		}
	}
	for _, marker := range []string{
		"[submodule \"third_party/discordgo\"]",
		"url = https://github.com/cartridge-gg/discordgo.git",
	} {
		if strings.Contains(gitmodules, marker) {
			t.Fatalf(".gitmodules still contains inaccessible discordgo submodule marker %q", marker)
		}
	}
	for _, path := range []string{
		filepath.Join(root, "third_party", "discordgo", ".git"),
		filepath.Join(root, "third_party", "discordgo", ".gitmodules"),
	} {
		if _, err := os.Stat(path); err == nil {
			t.Fatalf("legacy discordgo submodule metadata still exists at %s", path)
		} else if !os.IsNotExist(err) {
			t.Fatalf("stat legacy discordgo submodule metadata %s: %v", path, err)
		}
	}

	ci := read(".github", "workflows", "ci.yml")
	for _, marker := range []string{
		"submodules: recursive",
		"Build pinned libdave for DAVE voice tests",
		"make -C third_party/discordgo/dave/libdave/cpp BUILD_TYPE=Release",
		"CGO_ENABLED=1",
		"libdave.a",
	} {
		if !strings.Contains(ci, marker) {
			t.Fatalf("CI is missing DAVE build marker %q", marker)
		}
	}

	release := read(".github", "workflows", "release-host.yml")
	for _, marker := range []string{
		"submodules: recursive",
		"Build pinned libdave for release architectures",
		"triplet=x64-linux",
		"triplet=arm64-linux",
		"VCPKG_OVERLAY_TRIPLETS",
		"CGO_ENABLED=1",
		"libdave.a",
	} {
		if !strings.Contains(release, marker) {
			t.Fatalf("release workflow is missing DAVE build marker %q", marker)
		}
	}

	dockerfile := read("Dockerfile")
	for _, marker := range []string{
		"COPY third_party ./third_party",
		"RUN make -C third_party/discordgo/dave/libdave/cpp BUILD_TYPE=Release",
		"CGO_ENABLED=1",
		"libdave.a",
	} {
		if !strings.Contains(dockerfile, marker) {
			t.Fatalf("Dockerfile is missing DAVE build marker %q", marker)
		}
	}
}
