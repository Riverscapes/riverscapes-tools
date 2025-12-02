# Riverscapes Tools Documentation (Docusaurus)

## Writing docs for Riverscapes Tools Documentation in your local environment

This project uses [Docusaurus](https://docusaurus.io/) for documentation. You can develop, preview, and build the docs site in your local environment.

NOTE: You need to have Node.js>=22 installed locally to run this project.

### Getting Started locally

1. **Install Dependencies**  
   From the `/docs` directory, install dependencies:

   ```sh
   cd docs # only if not already in the docs directory
   yarn install
   ```

2. **Start the Development Server**  
   Run the Docusaurus dev server to preview changes live:

   ```sh
   yarn start
   ```

   The site will be available at `http://localhost:3000` by default. Open this URL in your browser to view the documentation site.

## Updating Layer Definition Layers

The docs site reads each tool's `layer_definitions.json` files from `docs/static/layers`. Those files are not copied; they are symlinks that point back into `packages/<tool>/<tool>/layer_definitions.json`, ensuring the docs stay in sync with the authoritative sources.

### Why symlinks?

This approach avoids duplication and ensures that any updates to layer definitions in the tool packages are automatically reflected in the documentation.

The symlinks allow us to preview the site locally instead of only being part of a build/launch workflow. They are committed to git. The symlinks become real files when the site is built for production, so they work correctly when deployed.

#### Getting symlinks from git

May require `git config --global core.symlinks true`

* On Windows, re-clone the repo from Terminal run *as Administrator*.

1. **Regenerate symlinks**  
   From the ./docs folder, run the link_layers.sh script:

   ```sh
   cd docs
   ./scripts/link_layers.sh          # optionally pass a custom filename pattern
   ```

   The script scans every package for files matching the pattern (defaults to `layer_definitions.json`) and creates `<tool>_<filename>.json` symlinks inside `docs/static/layers`.

2. **Commit the symlinks**  
   Git tracks symlinks as lightweight entries, so commit them like any other file:

   ```sh
   git add docs/static/layers
   git status                         # verify the new/updated links are listed as symbolic links
   git commit -m "Update layer definition links"
   ```

   If a link shows up as a regular file your platform may not support symlinks—double-check before committing.

3. **Handling broken symlinks**  
   When a source file moves or is removed you may see broken links (they usually appear red in `ls -l`). Clean them up with:

   ```sh
   find docs/static/layers -xtype l -delete
   ./scripts/link_layers.sh
   ```

   After regenerating, rerun `yarn start` (or the static build) to make sure the docs still render the layer tables correctly.

## Writing docs for Riverscapes Tools Documentation in GitHub Codespaces

This project uses [Docusaurus](https://docusaurus.io/) for documentation. You can develop, preview, and build the docs site directly inside a GitHub Codespace.

### Getting Started

1. **Open the Codespace**  
   Launch a Codespace for this repository from GitHub.

2. **Install Dependencies**  
   The dev container comes with Node.js and npm pre-installed. From the `/docs` directory, install dependencies:

   ```sh
   cd docs # only if not already in the docs directory
   yarn install
   ```

3. **Start the Development Server**  
   Run the Docusaurus dev server to preview changes live:
   ```sh
   yarn start
   ```
   The site will be available at the forwarded port (usually 3000). Use the "Ports" tab in Codespaces to open it in your browser.


4. **Directory Structure**
   - `docs/` – Markdown/MDX documentation files
   - `src/` – Custom React components and pages
   - `static/` – Static assets (images, etc.)
   - `docusaurus.config.ts` – Docusaurus configuration
