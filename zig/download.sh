#!/usr/bin/env sh
set -eu

ZIG_MIRROR="https://ziglang.org/builds"
ZIG_RELEASE="0.17.0-dev.956+2dca73595"
ZIG_CHECKSUMS=$(cat<<EOF
${ZIG_MIRROR}/zig-aarch64-linux-${ZIG_RELEASE}.tar.xz 2c04583b83aa25ef3602cd26fb7b63a8bab787b25a1e731ae0c77528a836052e
${ZIG_MIRROR}/zig-aarch64-macos-${ZIG_RELEASE}.tar.xz 7b85e126eda9c64085cbd63436055cc4e0535b743d8c753918fcc94b7e993bb8
${ZIG_MIRROR}/zig-aarch64-windows-${ZIG_RELEASE}.zip b6cc2b0ab343e1e053c730f237ae71eccc7b1f7bff9682b9f18aaf874cd2f18f
${ZIG_MIRROR}/zig-x86_64-linux-${ZIG_RELEASE}.tar.xz 11d995f47f847c3394bb8555d7d8841e686ac6ac8c521c8ce62a6616e6eb0b48
${ZIG_MIRROR}/zig-x86_64-macos-${ZIG_RELEASE}.tar.xz b1f7b93e02eb878f48e73b6f4567fa544e83bdf91e23d043ae69fea8367d9818
${ZIG_MIRROR}/zig-x86_64-windows-${ZIG_RELEASE}.zip e0d8aa07e8dfac41a6e835a17bfd4578ff979a2e6b0d57c6236c82391f26628c
EOF
)

# Determine the architecture:
if [ "$(uname -m)" = 'arm64' ] || [ "$(uname -m)" = 'aarch64' ]; then
    ZIG_ARCH="aarch64"
else
    ZIG_ARCH="x86_64"
fi

# Determine the operating system:
case "$(uname)" in
    Linux)
        ZIG_OS="linux"
        ZIG_EXTENSION=".tar.xz"
        ;;
    Darwin)
        ZIG_OS="macos"
        ZIG_EXTENSION=".tar.xz"
        ;;
    CYGWIN*)
        ZIG_OS="windows"
        ZIG_EXTENSION=".zip"
        ;;
    *)
        echo "Unknown OS"
        exit 1
        ;;
esac

ZIG_URL="${ZIG_MIRROR}/zig-${ZIG_ARCH}-${ZIG_OS}-${ZIG_RELEASE}${ZIG_EXTENSION}"
ZIG_CHECKSUM_EXPECTED=$(echo "$ZIG_CHECKSUMS" | grep -F "$ZIG_URL" | cut -d ' ' -f 2)

# Work out the filename from the URL, as well as the directory without the ".tar.xz" file extension:
ZIG_ARCHIVE="./zig/cache/$(basename "$ZIG_URL")"
ZIG_DIRECTORY=$(basename "$ZIG_ARCHIVE" "$ZIG_EXTENSION")

# Returns 0 if the given file exists and its SHA-256 checksum matches the expected value.
checksum_valid() {
    [ -f "$ZIG_ARCHIVE" ] || return 1
    ZIG_CHECKSUM_ACTUAL=""
    if command -v sha256sum > /dev/null; then
        ZIG_CHECKSUM_ACTUAL=$(sha256sum "$ZIG_ARCHIVE" | cut -d ' ' -f 1)
    elif command -v shasum > /dev/null; then
        ZIG_CHECKSUM_ACTUAL=$(shasum -a 256 "$ZIG_ARCHIVE" | cut -d ' ' -f 1)
    else
        echo "Neither sha256sum nor shasum available."
        exit 1
    fi
    [ "$ZIG_CHECKSUM_ACTUAL" = "$ZIG_CHECKSUM_EXPECTED" ]
}

if checksum_valid; then # Caching for CI.
    echo "Skip downloading Zig $ZIG_RELEASE."
else
    echo "Downloading Zig $ZIG_RELEASE ..."
    mkdir -p ./zig/cache
    # Download, making sure we download to the same output document, without
    # wget adding "-1" etc. if the file was previously partially downloaded:
    if command -v curl > /dev/null; then
        curl --location --silent --show-error --output "$ZIG_ARCHIVE" "$ZIG_URL"
    elif command -v wget > /dev/null; then
        # -4 forces `wget` to connect to ipv4 addresses, as ipv6 fails to resolve on certain distros.
        # Only A records (for ipv4) are used in DNS:
        ipv4="-4"
        # But Alpine doesn't support this argument
        if [ -f /etc/alpine-release ]; then
            ipv4=""
        fi

        # shellcheck disable=SC2086 # We control ipv4 and it'll always either be empty or -4
        wget $ipv4 --quiet --output-document="$ZIG_ARCHIVE" "$ZIG_URL"
    else
        echo "Neither curl nor wget available."
        exit 1
    fi

    # Verify the checksum.
    if ! checksum_valid; then
        echo "Checksum mismatch."
        exit 1
    fi
fi

echo "Extracting $ZIG_ARCHIVE ..."
case "$ZIG_EXTENSION" in
    ".tar.xz")
        tar -xf "$ZIG_ARCHIVE"
        ;;
    ".zip")
        unzip -q "$ZIG_ARCHIVE"
        ;;
    *)
        echo "Unexpected error extracting Zig archive."
        exit 1
        ;;
esac
# NB: Keep archive for caching.

# Replace these existing directories and files so that we can install or upgrade:
rm -rf zig/doc
rm -rf zig/lib
mv "$ZIG_DIRECTORY/LICENSE" zig/
mv "$ZIG_DIRECTORY/README.md" zig/
mv "$ZIG_DIRECTORY/doc" zig/
mv "$ZIG_DIRECTORY/lib" zig/
mv "$ZIG_DIRECTORY/zig" zig/

# We expect to have now moved all directories and files out of the extracted directory.
# Do not force remove so that we can get an error if the above list of files ever changes:
rmdir "$ZIG_DIRECTORY"

# It's up to the user to add this to their path if they want to:
ZIG_BIN="$(pwd)/zig/zig"
echo "Downloading completed ($ZIG_BIN)! Enjoy!"
