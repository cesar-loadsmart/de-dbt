dbt_dir = $(shell pwd)/de_dbt
dbt_dir_single = $(shell pwd)

dbt-debug:
	dbt debug --profiles-dir $(dbt_dir) --project-dir $(dbt_dir)

dbt-compile:
	dbt compile --profiles-dir $(dbt_dir) --project-dir $(dbt_dir) --exclude calendar
	cp de_dbt/target/manifest.json de_dbt

dbt-run:
	dbt run --profiles-dir $(dbt_dir) --project-dir $(dbt_dir) --exclude calendar

dbt-run-model:
	dbt run --models $(subst de_dbt/,,$(model)) --profiles-dir $(dbt_dir) --project-dir $(dbt_dir) --full-refresh

dbt-run-full:
	dbt run --profiles-dir ${dbt_dir} --project-dir ${dbt_dir} --full-refresh --exclude calendar

dbt-docs-serve:
	dbt docs generate --profiles-dir $(dbt_dir) --project-dir $(dbt_dir)
	dbt docs serve --profiles-dir $(dbt_dir) --project-dir $(dbt_dir) --port 8085

build-de-dbt:
	podman build -f docker_dbt/Dockerfile -t de_dbt  . 

run-full-qa:
	podman run -it --rm --env-file=.env --entrypoint /bin/bash de_dbt  -c "cd /app/de_dbt && dbt --partial-parse run --profiles-dir profiles_ci_qa --models ${model} --full-refresh" 

run-qa:
	podman run -it --rm --env-file=.env --entrypoint /bin/bash de_dbt  -c "cd /app/de_dbt && dbt --partial-parse run --profiles-dir profiles_ci_qa --models ${model}"

run-dbt-full-fal-qa:
	podman run -it --rm --env-file=.env --entrypoint /bin/bash de_dbt  -c "cd /app/de_dbt && dbt --partial-parse run --profiles-dir profiles_ci_qa --models ${model} --full-refresh && fal run --profiles-dir profiles_ci_qa" 

run-dbt-fal-qa:
	podman run -it --rm --env-file=.env --entrypoint /bin/bash de_dbt  -c "cd /app/de_dbt && dbt --partial-parse run --profiles-dir profiles_ci_qa --models ${model} && fal run --profiles-dir profiles_ci_qa" 

run-dbt-clean:
	podman run -it --rm --env-file=.env --entrypoint /bin/bash de_dbt  -c "cd /app/de_dbt && dbt clean" 

run-qa-tagged:
	podman run -it --rm --env-file=.env --entrypoint /bin/bash de_dbt  -c "cd /app/de_dbt && dbt --partial-parse run --profiles-dir profiles_ci_qa --models tag:${tag}"

run-full-qa-tagged:
	podman run -it --rm --env-file=.env --entrypoint /bin/bash de_dbt  -c "cd /app/de_dbt && dbt --partial-parse run --profiles-dir profiles_ci_qa --models tag:${tag} --full-refresh"
