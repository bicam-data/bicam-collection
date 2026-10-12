# Directory extraction regression excerpt

`aderholt-119.txt` preserves the full Aderholt biography and office URL from
the captured official Congress-119 directory, with minimal state context.
The header context is shortened; the final roster sentinel is synthetic.
The internal phrase `Republican National Convention` must not be mistaken for
a second biography boundary.

Official source:
https://www.govinfo.gov/content/pkg/CDIR-2026-02-20/text/CDIR-2026-02-20.txt

Captured whole-file SHA-256:
`7757cde4dc6cde3e7dc697bd12d841f6d9a159050769105b650e03f8c777d3e5`.
Full-source biography lines 712–719 and URL line 722, using one-based lines.
The government-authored directory text is used for extraction regression only.
