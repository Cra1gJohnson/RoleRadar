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
- - implemented deletion script for 404 errors
- - implemented validity check for previosly enriched jobs

- improve scoring
- - improved promp and with shorter context and lower cost

- improve question answering
- - began attempt at logic for question answering

- implement playwright and chrome connection
-- was able to get browser opening and some question fill

- add delete constraints to db
- - added cascade constraints for child tables of green_job.

## March 31

- improve playwright
- - mild improvements to playwright made

## April 1

- improve playwright
- - changes in the way application is prepared and how scripts in apply/ interact
- - large improvements to actual playwrite functionality
- - first application sent !!!

## April 2

- brainstorm how to make playwright fill script more multi purpose
- - using locator.count to detect fields

## April 4th

- coninue work on application preparation filling,
- - more playwright research

## April 6th

- continue playwright improvement

## April 7th

- finalize playwright improvement, what should I do about cookies?
- - expand green_apply to include job_id, prompt, model, response, resume, cover letter, packaged_at, submitted_at fields
- - begin application snapshot

## April 8th

- create application table
- rename enrichment scoring fields to `green_enrich.scored` and `green_score.scored_at`
- create utility script to transfer backlog to application table
- read about context caching
- attempt to implement custom cover letter logic

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
