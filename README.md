## Repository Overview

This project features an analytics dashboard solution hosted on AWS, designed to help decision-makers at a café restaurant chain identify sales trends. The implementation includes an ETL pipeline and a CI/CD workflow powered by GitHub Actions.

> ⚠️ **Note:** All personal data used within the project is entirely fictitious and intended solely for development purposes.

## Project Structure

- **`src/` folder** — contains the development version of the application, executed locally using Docker.
- **`cafesquad-*` folders** — store source code and packages deployed to AWS, utilizing native AWS Lambda functions to accelerate data loading into the Redshift database.
