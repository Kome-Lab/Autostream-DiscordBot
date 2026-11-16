#!/usr/bin/env bash
set -euo pipefail
: "${1:?explicit disposable DAVE environment output is required}"
msr_build_dave() {
set -euo pipefail
vcpkg_root="$PWD/third_party/discordgo/dave/libdave/cpp/vcpkg"
cpp_root="$PWD/third_party/discordgo/dave/libdave/cpp"
build_dir="$cpp_root/build"
missing_tool=0
for tool in cmake ninja g++ make pkg-config autoconf automake libtool zip unzip tar; do
  if ! command -v "$tool" >/dev/null 2>&1; then
    missing_tool=1
  fi
done
if [ "$missing_tool" -eq 1 ]; then
  sudo apt-get update
  sudo apt-get install -y --no-install-recommends \
    cmake ninja-build g++ make pkg-config autoconf automake libtool \
    zip unzip tar
fi
if [ ! -x "$vcpkg_root/vcpkg" ]; then
  git -C "$vcpkg_root" fetch --unshallow || git -C "$vcpkg_root" fetch --all --tags
  for bootstrap_attempt in 1 2 3; do
    echo "Bootstrapping vcpkg (attempt ${bootstrap_attempt}/3)"
    if "$vcpkg_root/bootstrap-vcpkg.sh" -disableMetrics; then
      break
    fi
    if [ "$bootstrap_attempt" -eq 3 ]; then
      echo 'vcpkg bootstrap failed after 3 attempts' >&2
      return 1
    fi
    sleep $((bootstrap_attempt * 15))
  done
fi
test -x "$vcpkg_root/vcpkg"
export CC=gcc CXX=g++
cache_hit="${MSR_DAVE_CACHE_HIT:-false}"
if [ "$cache_hit" = "true" ] && [ -f "$build_dir/libdave.a" ]; then
  echo 'Using exact cached libdave build.'
  return 0
fi
# Hosted-runner GitHub archive downloads can reset transiently; retain
# completed vcpkg packages and retry the configure/build as a unit.
# This is the explicit-CMake equivalent of:
# make -C third_party/discordgo/dave/libdave/cpp BUILD_TYPE=Release
for attempt in 1 2 3; do
  echo "Building pinned libdave (attempt ${attempt}/3)"
  if cmake -S "$cpp_root" -B "$build_dir" -G Ninja \
    -DCMAKE_BUILD_TYPE=Release \
    -DVCPKG_MANIFEST_DIR="$cpp_root/vcpkg-alts/openssl_3" \
    -DCMAKE_TOOLCHAIN_FILE="$cpp_root/vcpkg/scripts/buildsystems/vcpkg.cmake" \
    -DBUILD_SHARED_LIBS=OFF \
    -DENABLE_SANITIZERS=OFF \
    -DPERSISTENT_KEYS=OFF \
    -DTESTING=OFF \
    -DCMAKE_INSTALL_PREFIX="$build_dir/install" \
    -DINSTALL_VCPKG_LICENSES=OFF \
    && cmake --build "$build_dir" --target libdave --config Release; then
    return 0
  fi
  if [ "$attempt" -eq 3 ]; then
    echo 'libdave build failed after 3 attempts' >&2
    return 1
  fi
  sleep $((attempt * 15))
done

}
msr_build_dave
libdave_build="$PWD/third_party/discordgo/dave/libdave/cpp/build"
vcpkg_libdir="$(find "$libdave_build/vcpkg_installed" -maxdepth 2 -type d -name lib -print -quit)"
test -n "$vcpkg_libdir"
vcpkg_libs="$(find "$vcpkg_libdir" -maxdepth 1 -type f -name '*.a' -print | sort | tr '\n' ' ')"
printf 'export CGO_ENABLED=1\nexport CGO_CFLAGS=%q\nexport CGO_LDFLAGS=%q\n' "-I$PWD/third_party/discordgo/dave/libdave/cpp/includes" "-L$vcpkg_libdir $libdave_build/libdave.a -Wl,--start-group $vcpkg_libs -Wl,--end-group -lstdc++ -lm -ldl -lpthread" > "$1"
