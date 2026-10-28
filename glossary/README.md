# Business glossary for the Statbel open data (OpenMetadata import)

Five glossaries and **127 business terms** built with the method of the
guideline **DG-GDL-001 — Building a business data catalog** (Olivier
Bouchez): business words first, one concept one term, share before you
specialise, stop where the business stops.

| Glossary | Domain (L0) | Content |
|---|---|---|
| Shared Business Vocabulary | Cross-domain | Party, Person, Organisation, Location, Address, Time, shared classifications |
| Population and Households | Population and Society | resident population, households, vital events, places of residence and birth |
| Enterprises and Economy | Economy | enterprises, VAT, economic activity, bankruptcies |
| Mobility and Road Safety | Mobility | road accidents, victims, circumstances |
| Buildings and Real Estate | Built Environment | buildings, dwellings, real estate transactions |

Each term carries a **type** (tag of the classification `TermType`:
SubjectArea, BusinessObject, Characteristic, Role, Measure, Classification,
Identifier), a definition, French and Dutch synonyms and, as mapping
evidence, the **Statbel source columns** it explains. Eleven terms are
tagged `PersonalData.Personal`.

See the result, readable without an account:
**[olivier-bouchez.com/data-catalog-demo](https://olivier-bouchez.com/data-catalog-demo)**.

## Files

```
openmetadata-import/
  pass1-terms/            the 127 terms, without related terms
  pass2-related-terms/    the 42 terms that have related terms
```

Format: glossary import of **OpenMetadata 2.0** (tested on 2.0.6), 15
columns in this order: `parent, name*, displayName, description, synonyms,
relatedTerms, references, tags, reviewers, owner, glossaryStatus, color,
iconURL, domains, extension`.

## Import into OpenMetadata

1. Create the classification **`TermType`** with its seven tags (and the
   classification `PersonalData` with the tag `Personal`, provided by
   OpenMetadata).
2. Create the five **domains** and their Data Owners.
3. Create the five **glossaries** with exactly the names above (spaces and
   capitals included). Do not add reviewers yet: with reviewers in place,
   every imported term opens an approval task.
4. **Pass 1**, glossary by glossary, starting with *Shared Business
   Vocabulary*: open the glossary › ⋮ › **Import** › file of `pass1-terms`
   › check the preview › Import.
5. **Pass 2**, same order, files of `pass2-related-terms`: the 42 terms are
   updated with their related terms (they reference terms created in pass
   1, possibly in another glossary).
6. Add the reviewers, then run the terms through the approval workflow
   (all terms are imported as `Draft`).

Lessons learned: compare the header with an export of an empty glossary of
your instance (the format changes between releases); a first-level term has
an **empty** `parent`; the last column of the import preview gives the
reason of each failed row.

## Licence

© 2026 Olivier Bouchez. Licensed under
[CC BY-NC 4.0](https://creativecommons.org/licenses/by-nc/4.0/): free for
non-commercial use, with attribution. The definitions build on Statbel open
data metadata (CC BY 4.0).
