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

## April 20th

- git history changing
- GH remote change

## April 21st

- latex starter files for software dev
- resume
- and coverletter

## April 22nd

- refresh entire snapshot
- - async implemented in snapshot refresh and rpm bumped to 200
- create utility script to print jobs at each stage for monitoring
- - script created under utility, prints counts from each table

- implement answer caching and approval for application packaging

- push for ashby addition

## April 24th

- fixed score jobs bug in psycopg connection parameters.

## April 25th

- improve prepare_app to be async and add token and cost calculation
- implement text_area and input_text storage and context usage in DB and prompt
- refresh package review with ability change in nvim EDITOR before storage

## April 26th

- found bug in app preparation
- implement better cli interface for app approval

## May 1st

- Collection changes to location flag on green_job table insert
- Create_ scripts for table creation, moving to ats agnostic schema
- Dockerfile creation and begining of testing for server use.
- repo clean

## May 2nd

- ensured db uses lower() board names for Green_and Ashby, case-sensitive for lever
- Added Create snapshot and job tables

## May 3rd

-

## Next things

- candidate_filter needs to do the regex inside of psql, Because It takes far too long outside.
- Implement Pre-validation ats_board check in you_search.py
- discovery.md updates
- solidify lower board names in db
- Collection that is ats agnostic
- dockerize collection
- dockerize postgres

## ideas towards docker

- one container for collection and network requests
- one container for postgres
- one network to share collection and DB
- one container for app-user

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
