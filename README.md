# de-dbt
This is an Odin/Odinprep related project. It was created to separate Engineering DM models from other business data models. Therefore, this repository will be managed and maintained by the Data Engineering Squadron. It will run independently in a separate dag, preventing problems when running Odin or Odinprep.

### Using the DE-DBT project

Try running the following commands:

- make build-de-dbt: to build the docker image with the de-dbt project.
- make run-dbt-fal-qa: to run a selected model and the fal job (if exists for the specific model)
- make run-qa-tagged: to run all models with the selected tag


### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices