---
title: Running with Docker
---

This repo comes with a Dockerfile that you should be able to use to run these tools without any prerequisites (except docker of course)

### building

1. Run `./scripts/dockerBuild.sh`
2. Make a script to run (i.e. `./scripts/testscript.sh`)
3. Create an `.env`
4. `./scripts/dockerRun.sh ~/Work/data/bratDockerShare ./scripts/testscript.sh `