# Official source regression fixtures

Retrieved September 8, 2026; source-owned content, not generated mock prose.

- `eh1s.mods.xml`: https://www.govinfo.gov/metadata/pkg/BILLS-119hr3426eh1s/mods.xml
- `eh1s.html`: https://www.govinfo.gov/content/pkg/BILLS-119hr3426eh1s/html/BILLS-119hr3426eh1s.htm
- `hres15-109.contradictory.html`: https://www.congress.gov/109/bills/hres15/BILLS-109hres15ih.htm
- `hres15-109.ih.xml`: https://www.congress.gov/109/bills/hres15/BILLS-109hres15ih.xml

The star print has semantic EH, exact suffix eh1s, star-print number 1, and
no legacy numeric variant. The H.Res.15 HTML has the wrong Congress/body;
the same-bill IH XML is verified against Congress metadata and its text list.
Preserve these documents as evidence; never fix the HTML by replacing its
Congress number. A future XML-derived extraction must retain its own source.
Source files use UTF-8 and a single terminal newline.
