# Project Improvement Schedule

## Pre-March 30th

`reference git commits to see build timeline`

Summary:

- Board token discovery
- snapshot collection
- job normalization
- general title filtering
- job enrichment basic protocol
- scoring basic protocol and AI implementation
- AI question answering
- - reference database.md for table descriptions

## March 30

Tasks:  

- improve snapshot validity
- improve scoring
- improve question answering
- implement playwright and chrome connection
- add delete constraints to db

## Commit Guidelines

Conventional Commits
use a structured `type(scope): description` format.

Common Commit Types:

- `<feat>`: A new feature.
- `<fix>`: A bug fix.
- `<docs>`: Documentation only changes.
- `<style>`: Changes that do not affect the meaning of the code formating white space.
- `<refactor>`: A code change that neither fixes a bug nor adds a feature.
- `<perf>`: A code change that improves performance.
- `<test>`: Adding missing tests or correcting existing tests.
- `<chore>`: Routine tasks like updating dependencies or build scripts.
