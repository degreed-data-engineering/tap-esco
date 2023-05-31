# tap-esco
This tap ESCO was created by Degreed to be used for extracting data via Meltano into defined targets.

# Configuration required:

No configuration is required.
## Testing locally

To test locally, pipx poetry
```bash
pipx install poetry
```

Install poetry for the package
```bash
poetry install
```

To confirm everything is setup properly, run the following: 
```bash
poetry run tap-esco --help
```

To run the tap locally outside of Meltano and view the response in a text file, run the following: 
```bash
poetry run tap-esco > output.txt 
```

A full list of supported settings and capabilities is available by running: `tap-lightcast --about`