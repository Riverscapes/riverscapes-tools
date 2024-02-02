# Clean template site

## Getting started for writing content

1. Open up the `Docs.code-workspace` file in VSCode. This should open up the workspace and put you in the right place to run the site.
2. Make sure you are running node > v18 and have access to yarn at the command line

```bash
> node --version
v18.16.0

> yarn --version
3.6.1
```

3. If yarn insn't found you may need to install it: `npm install --global yarn`

***The version of yarn isn't that important for content writers. We just run this command to make sure it's installed.***

4. Open up a terminal window in VSCode and run `yarn install` to install all the dependencies.
5. Run `yarn start` to start the dev server and develop locally.
6. If you think something is broken or weird you can run `yarn clean` to clear the cache and then `yarn start` again to see if that fixes it. We also have `yarn start:clean` that combines these two steps.


## Creating a new `./docs` site

1. Copy everything (excluding `node_modules`, `.cache` and `public`) in this `./template` folder to a `./docs` subfolder in your desired repo. **NOTE: Make SURE to get all the dot-prefixed hidden files and folders includeing `.vscode`.**
2. Open the `Docs.code-workspace` in the new location. 
   - *You may need to open the workspace file and adjust the paths inside it if you move it to a different location.*
   - For those of us with nvm installed (typically non-windows users) This should put your terminal in the right place so that the `.nvmrc` file registers and the correct version of node is installed. 
3. open `gatsby-config.ts` and change all the **TODO** items:
  - `pathPrefix`: If this site is going to live at a subpath like `https://example.com/useless-site`, then change this to `/useless-site` **NOTE: Case matters**
  - `start_url`: Should match the `pathPrefix` if you have one
4. Open a terminal and make sure you are in the `./docs` folder with the correct version of Nodejs
5. Run `yarn install` inside the `./docs` folder to install all the dependencies
6. RUn `yarn start` inside the `./docs` folder to start the dev server and develop locally.
7. Now move the 2 yml files in the root of your `.git` repo. They are living in the `.github/workflows` folder next to this README.md file but they will need to be moved to the root of whatever repo they end up in.
   1. `/.github/workflows/pages-publish.yml`
   2. `/.github/workflows/pages-validate.yml`
8. Move the entire `.devcontainer` folder to the root of your repo. This is the folder that contains the `Dockerfile` and `devcontainer.json` files.
9. Once you push these files push the whole mess to Github and then go to the repo settings and enable Github Pages:
   1. Repo Settings
   2. Pages
   3. Build and deployment --> Source --> Select `Github Actions`
   4. Wait for it to build and deploy.

## Upgrading a site to the latest version of Gatsby

1. Open a terminal and make sure you are in the `./docs` folder with the correct version of Nodejs.
2. Run `yarn upgrade:theme` (it's a custom script) to upgrade to the latest version of Gatsby.
3. If you like, check your `yarn.lock` file in the git diff to make sure the theme is at the correct level.

## Migration of Jekyll `./docs` site to Gatsby

1. Delete the old Jekyll files:
   - `Gemfile`
   - `.gitignore` (The steps above include a new, better Gatsby .gitignore)
   - `_config.yml`
2. Move all remaining files into a temporary `ROOT/docs/OLD_JEKYLL` folder so that gatsby will ignore it for now. We do this because Jekyll allows you to use files in any location and it can be confusing to have them in the same folder as the new Gatsby files. We want to move them out of the way for now and then bring them back one-by-one as we fix them.
2. One-by-one move each existing markdown files from the `ROOT/Docs/OLD_JEKYLL` folder into the `content/page` folder. It is recommended to make small git commits as you go so it's easier to see what changed and what broke the build. ***NEVER COMMIT ANYTHING BROKEN***
   1. Change file extension from `*.md` to `*.mdx` 
   2. Fix the frontmatter (see below).
   3. Fix the markdown content by visiting the local page in the browser and looking for errors on the screen and in the console (common problems are listed below).
   4. Move any images the page depends on from the `assets/images` folder to the `static/images` folder.


## Typical problems and content migration gotchas:

- Look for liquid tags that are left over from jekyll: `{{ some.liquid.tag }}` these need to be removed.
- Any changes to `gatsby-config.ts` will require a restart of the dev server.
- Unclosed img tags: `<img src="some-image.jpg">` is not allowed. You need to close the tag like this `<img src="some-image.jpg" />`
- HTML Comments: `<!-- This is a comment -->` are not allowed and will break the site. You need to use MDX comments like this `{/* This is a comment */}`
- Frontmatter Fields:
    - All frontmatter is optional. You can have no frontmatter at all and the page will still work.
    - `title` is optional. It is used in conjunction with `banner`
    - `description` is optional. It is used in conjunction with `banner`
    - If you have `isHome` set to true (you can ommit this completely if it's false) Then you need to wrap all your content in a container tag.
    - If you want the banner at the top of the page set `banner:true`
    - `banner` and `isHome` do not work well together
    - `image`: This is the image that will be used for this page as a thumbnail in the lower image cards below the content.
    - `imageAlt`: Related to the `image` field. This is the alt text for the image.

```mdx
import { Container } from '@mui/material'

<Container maxWidth="xl">

## This is a title

here is some content

</Container>
```


- Menu links: 
    - **Broken menu links** (i.e. links that lead to a non-existent page) will not crash the site. They will just lead to a 404. On the 404 page you should be able to search for the correct url using the search form.
- Images: 
    - If you import images like `import WhatIsRiverscape from './what-is-riverscape.jpg'` then the image not being there WILL break the site. It will look like a red error in the console

```
  ModuleNotFoundError: Module not found: Error: Can't resolve './what-is-riverscape.jpg' in '/some/path/content/page'
```
