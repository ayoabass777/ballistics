# Inter (local build asset)

The app already used Inter via `next/font/google`. Its remote build-time requests
blocked local page compilation, so the same font family is now bundled with
`next/font/local`. No runtime Google Fonts request is required.

Source retrieved 2026-10-04:
https://raw.githubusercontent.com/google/fonts/main/ofl/inter/Inter%5Bopsz%2Cwght%5D.ttf

`Inter-variable-latin.woff2` (165 KB) is that file cut down to Latin, Latin
Extended, punctuation and currency glyphs and saved as WOFF2. The full TTF was
877 KB (469 KB over the wire) and loaded on every page. Both variable axes stay:
weights 100–900 and optical size 14–32. Other scripts fall back to the system font.

Rebuild it from the source TTF with fontTools:

    python3 -m fontTools.subset Inter-variable.ttf \
      --output-file=Inter-variable-latin.woff2 --flavor=woff2 --layout-features='*' \
      --unicodes="U+0000-00FF,U+0100-024F,U+0259,U+02BB-02BC,U+02C6,U+02DA,U+02DC,U+1E00-1EFF,U+2000-206F,U+20AC,U+20B9,U+2122,U+2190-2199,U+2212,U+2215,U+FEFF,U+FFFD"

Original license is included in `OFL.txt`.
