# Inter (local build asset)

The app already used Inter via `next/font/google`. Its remote build-time requests
blocked local page compilation, so the same font family is now bundled with
`next/font/local`. No runtime Google Fonts request is required.

Source retrieved 2026-10-04:
https://raw.githubusercontent.com/google/fonts/main/ofl/inter/Inter%5Bopsz%2Cwght%5D.ttf

Full normal-style variable font, including international glyphs and weights
100–900. Original license is included in `OFL.txt`.
