# Template für Abschlussarbeiten - Informatik JGU
Das Template basiert auf der Arbeit von https://github.com/derric/cleanthesis und wird in diesem Repository an die Bedürfnisse der JGU und des Instituts für Informatik angepasst. 
Insbesondere zählt dazu die Integration der JGU Farbe (rot), des JGU Logos und der Schriftart Noto. Die Titelseite wird für Bachelor-, Master- und Doktorarbeit je nach Einstellung angepasst. 

[![pipeline status](https://gitlab.rlp.net/institut-fur-informatik/cleanthesis-jgu/badges/main/pipeline.svg)](hhttps://gitlab.rlp.net/institut-fur-informatik/cleanthesis-jgu/-/jobs/artifacts/main/browse?job=build_pdf)

[Link to the artifacts page of the last successful pipeline run with the final built thesis PDF](https://gitlab.rlp.net/institut-fur-informatik/cleanthesis-jgu/-/jobs/artifacts/main/browse?job=build_pdf)

## Hinweise
- Metainformationen in der my-thesis-setup.tex anpassen. 
- Es wird das Latex noto package benötigt (enthalten im Ubuntu package: `texlive-fonts-extra`)
- Für einbinden eines Colorprofiles (damit Farben im Druck korrekt dargestellt werden) wird lualatex mit dem pdfx package verwendet. Dafür muss die Datei `FOGRA29_ISOuncoated.icc` im Hauptverzeichnis liegen (enthalten im Repository). (siehe auch Abschnitt "Farben")
- Die CI/CD Pipeline erstellt eine PDF mit lualatex und biber bei jedem neuen Commit. 
  - README.md anpassen mit dem richtigen Link zum Pipeline Status und zur PDF-Seite (Projekt-URL ersetzen).
- Bei mehreren Quellen mit gleichen Autoren werden mit dem Bibstyle `numeric` die zweite Nennung mit `-` eresetzt. Um die Autorenausgabe zu erzwingen, kann die Autorenauflistung in der `.bib` Datei mit `{{ }}` erzwungen werden oder es kann über die Option `bibstyle=<biblatex stil>` ein alternativer Zitierstil verwendet werden. Weitere Informationen im Issue #1. 

## Farben
Der Farbworkflow für Druck ist CMYK. Im Dokument sind die Logos des Instituts und der Universität mit FOGRA29 Farbprofil eingebunden. Damit die Farben auch im Druck korrekt dargestellt werden, muss das gleiche Farbprofil auch für die Dokumentfarben verwendet werden. Dafür wird das pdfx package mit lualatex verwendet. Wenn ein anderes Farbprofil verwendet werden soll, muss die Datei `FOGRA29_ISOuncoated.icc` durch das gewünschte Farbprofil ersetzt und die Zeile `\setRGBcolorprofile{FOGRA29_ISOuncoated.icc}{FOGRA29}{cmyk}` entsprechend angepasst werden. Die zugehörigen Bilddaten müssen ebenfalls mit dem gleichen Farbprofil eingebunden werden, dazu liegen die Logos als Affinity-Dateien im Repository, damit können die Logos mit dem richtigen Farbprofil exportiert werden. 

In den meisten Fällen ist Farbtreue jedoch weniger wichtig. Das einbinden des Farbprofils wie im Beispiel sorgt allerdings für konsistente Farben zwischen Dokument und Logos.

---

--- ORIGINAL README ---

# Clean Thesis
*-- A Clean LaTeX Style for Thesis Documents --*

## Table of contents

* [About](#about)
* [Getting started](#getting-started)
* [Documentation and Support](#documentation-and-support)
* [Contributing](#contributing)
* [License Information](#license-information)

## About

**Clean, Simple, Elegant**
*Clean Thesis* is a LaTeX style for thesis documents, developed for my diploma thesis (Diplomarbeit). The style can be understood as my personal compromise — a typical clean looking scientific document combined and polished with minor beautifications.

The design of this *Clean Thesis* style is inspired by user guide documents from Apple Inc.

Note: If you are looking for an exact and correct style regarding typographic rules, please have a look at the "[Classic Thesis Style](see http://www.miede.de/#classicthesis)".

**Donation = Postcard**
Based on the idea of André Miede: If you like the *Clean Thesis* style I would be very pleased about a donation in the form of a POSTCARD. You can find my address at the *Clean Thesis* [website](http://cleanthesis.der-ric.de/) as well as in the [documentation](doc/cleanthesis-doc.pdf). I am going to collect all postcards and exhibit them at the website I mentioned.

**Idea and Inspiration**
The idea of providing my customized style for thesis documents passed through my mind while writing my own thesis. Motivated and inspired by the superb "[Classic Thesis Style](http://www.miede.de/#classicthesis)" by André Miede (thanks to André for doing a great job) I decided to collect all design and style related functionality in a separate LaTeX style and provide this style to other thesis writers.

## Getting started

* Make sure you have an up-to-date TeX distribution
* Download the example-thesis from the [releases page](https://github.com/derric/cleanthesis/releases) (file `cleanthesis-incl-example_v****.zip`)
* [Check out the documentation](doc/cleanthesis-doc.pdf) to learn more about *Clean Thesis* and available package options

## Documentation and Support

The [documentation](doc/cleanthesis-doc.pdf) gives an overview of available package options.

Please use [Stack Exchange](http://tex.stackexchange.com/questions/tagged/cleanthesis) for questions about **using** *Clean Thesis*; and add the tag **cleanthesis** to your question. GitHub issues are for development purposes, e.g, reporting actual bugs or requesting new features.

## Contributing

Did you find an error or a bug? Do you have a feature request? Before opening a [new issue](https://github.com/derric/cleanthesis/issues/new), please check out the list of [existing](https://github.com/derric/cleanthesis/issues) and [closed](https://github.com/derric/cleanthesis/issues?q=is%3Aissue+is%3Aclosed) issues.

## License Information

README.md
Copyright 2020 R. Langner

This work may be distributed and/or modified under the
conditions of the LaTeX Project Public License, either version 1.3
of this license or (at your option) any later version.
The latest version of this license is in
  http://www.latex-project.org/lppl.txt
and version 1.3 or later is part of all distributions of LaTeX
version 2005/12/01 or later.

This work has the LPPL maintenance status `maintained'.

The Current Maintainer of this work is R. Langner.

This work consists of all files listed in MANIFEST.md.
