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
