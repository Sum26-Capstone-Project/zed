# Zed Voice Input

This fork adds local voice prompting to Zed's Agent panel. It combines native microphone capture in Zed with two local speech-to-text services: [realtime-stt-zed](https://github.com/Sum26-Capstone-Project/realtime-stt-zed) for dictation and [background-stt-zed](https://github.com/Sum26-Capstone-Project/background-stt-zed) for voice start/stop detection.

## Voice input

- Toggle the microphone in an Agent prompt to start or stop dictation.
- Stream partial and final transcriptions into the prompt without overwriting already finalised text.
- Select and persist a dictation language: English, Russian, French, German, Spanish, Chinese, Italian, Japanese, or Korean.
- Control dictation hands-free with the vocal phrases .
- Keep recognition local: Zed sends audio chunks to loopback WebSocket services.

```mermaid
flowchart LR
    U[User] -->|Initiated input or system detected start word| Z[Zed Agent panel]
    Z -->|Audio chunks| R[Realtime STT :8765]
    R -->|Partial / Final text| Z
    Z -->|Extracted prompt text| A[Agent]
    M[Microphone] -->|Audio chunks| D[Background detector :8764]
    D -->|Start word / Stop word| Z
```

The background detector starts after the Agent panel is initialised. A detected command affects only the active Zed window when it is showing an Agent thread. The selected language applies to realtime dictation, not to the English command detector. See the extension READMEs for runtime details.

---

# Upstream Zed

[![Zed](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/zed-industries/zed/main/assets/badge/v0.json)](https://zed.dev)
[![CI](https://github.com/zed-industries/zed/actions/workflows/run_tests.yml/badge.svg)](https://github.com/zed-industries/zed/actions/workflows/run_tests.yml)

Welcome to Zed, a high-performance, multiplayer code editor from the creators of [Atom](https://github.com/atom/atom) and [Tree-sitter](https://github.com/tree-sitter/tree-sitter).

---

### Installation

On macOS, Linux, and Windows you can [download Zed directly](https://zed.dev/download) or install Zed via your local package manager ([macOS](https://zed.dev/docs/installation#macos)/[Linux](https://zed.dev/docs/linux#installing-via-a-package-manager)/[Windows](https://zed.dev/docs/windows#package-managers)).

Other platforms are not yet available:

- Web ([tracking discussion](https://github.com/zed-industries/zed/discussions/26195))

### Developing Zed

- [Building Zed for macOS](./docs/src/development/macos.md)
- [Building Zed for Linux](./docs/src/development/linux.md)
- [Building Zed for Windows](./docs/src/development/windows.md)

### Contributing

See [CONTRIBUTING.md](./CONTRIBUTING.md) for ways you can contribute to Zed.

Also... we're hiring! Check out our [jobs](https://zed.dev/jobs) page for open roles.

### Licensing

Zed source code is licensed primarily under GPL-3.0-or-later, with Apache-2.0 components where marked.

License information for third party dependencies must be correctly provided for CI to pass.

We use [`cargo-about`](https://github.com/EmbarkStudios/cargo-about) to automatically comply with open source licenses. If CI is failing, check the following:

- Is it showing a `no license specified` error for a crate you've created? If so, add `publish = false` under `[package]` in your crate's Cargo.toml.
- Is the error `failed to satisfy license requirements` for a dependency? If so, first determine what license the project has and whether this system is sufficient to comply with this license's requirements. If you're unsure, ask a lawyer. Once you've verified that this system is acceptable add the license's SPDX identifier to the `accepted` array in `script/licenses/zed-licenses.toml`.
- Is `cargo-about` unable to find the license for a dependency? If so, add a clarification field at the end of `script/licenses/zed-licenses.toml`, as specified in the [cargo-about book](https://embarkstudios.github.io/cargo-about/cli/generate/config.html#crate-configuration).

## Sponsorship

Zed is developed by **Zed Industries, Inc.**, a for-profit company.

If you’d like to financially support the project, you can do so via GitHub Sponsors.
Sponsorships go directly to Zed Industries and are used as general company revenue.
There are no perks or entitlements associated with sponsorship.
