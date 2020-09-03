
## Where to find things

For overall [Riverscapes Documentation](http://riverscapes.northarrowresearch.com/), see the following techncial references:
* [Guidelines for Documenting a Riverscapes Model](http://riverscapes.northarrowresearch.com/Technical_Reference/how_to_document_a_model.html)
* [Cheatsheets on Jekyll Toolbox Documentation](http://riverscapes.northarrowresearch.com/Technical_Reference/jekyll_toolbox.html)- this is how you get the page titles (`title: Page Title`) to work, the proper behavior in the site table of contents, and a whole host of other issues.
* [Theming a Jekyll Site](http://riverscapes.northarrowresearch.com/Technical_Reference/applying_theme.html) - This is how to apply the Riverscapes theme to your site.

### Misc problems

* if you see '%20' in your sidebar it's because you used spaces in your folder names. Switch to underscores

### Site templates and HTML

* The default template that controls the large-scale page layout is:  `_layouts/default.html`. There can be more than one but we like to keep it simple
* The footer, sidenav and navigation stuff is in `_includes/`. We build a lot of stuff using javascript so `_includes/navigation.html` is going to not be very useful.

### Editing CSS:

* `docs/assets/css/custom.css` for custom hackery and small CSS changes


## Updating your favicon

Favicons used to be simple. Now, with the range of resolutions they have to support things have gotten more complicated. Here's how you generate your own

1. Find a nice sized copy of your logo that is square.
2. Go to a service like http://www.favicon-generator.org/ and upload it. It will give you the following files in a zip: `android-icon-144x144.png`, `apple-icon-114x114.png`, `apple-icon-60x60.png`, `favicon-16x16.png`, `ms-icon-150x150.png`, `android-icon-192x192.png`, `apple-icon-120x120.png`, `apple-icon-72x72.png`, `favicon-32x32.png`, `ms-icon-310x310.png`, `android-icon-36x36.png`, `apple-icon-144x144.png`, `apple-icon-76x76.png`, `favicon-96x96.png`, `ms-icon-70x70.png`, `android-icon-48x48.png`, `apple-icon-152x152.png`, `apple-icon-precomposed.png`, `favicon.ico`, `android-icon-72x72.png`, `apple-icon-180x180.png`, `apple-icon.png`, `manifest.json`, `android-icon-96x96.png`, `apple-icon-57x57.png`, `browserconfig.xml`, `ms-icon-144x144.png`
3. put these files in the folder `assets/images/favicons`

Now you're ready to link them up. Open `src/_layouts/default.html` and add the following lines:

```html
      <link rel="apple-touch-icon" sizes="57x57" href="{{ site.baseurl }}/assets/images/favicons/apple-icon-57x57.png">
      <link rel="apple-touch-icon" sizes="60x60" href="{{ site.baseurl }}/assets/images/favicons/apple-icon-60x60.png">
      <link rel="apple-touch-icon" sizes="72x72" href="{{ site.baseurl }}/assets/images/favicons/apple-icon-72x72.png">
      <link rel="apple-touch-icon" sizes="76x76" href="{{ site.baseurl }}/assets/images/favicons/apple-icon-76x76.png">
      <link rel="apple-touch-icon" sizes="114x114" href="{{ site.baseurl }}/assets/images/favicons/apple-icon-114x114.png">
      <link rel="apple-touch-icon" sizes="120x120" href="{{ site.baseurl }}/assets/images/favicons/apple-icon-120x120.png">
      <link rel="apple-touch-icon" sizes="144x144" href="{{ site.baseurl }}/assets/images/favicons/apple-icon-144x144.png">
      <link rel="apple-touch-icon" sizes="152x152" href="{{ site.baseurl }}/assets/images/favicons/apple-icon-152x152.png">
      <link rel="apple-touch-icon" sizes="180x180" href="{{ site.baseurl }}/assets/images/favicons/apple-icon-180x180.png">
      <link rel="icon" type="image/png" sizes="192x192"  href="{{ site.baseurl }}/assets/images/favicons/android-icon-192x192.png">
      <link rel="icon" type="image/png" sizes="32x32" href="{{ site.baseurl }}/assets/images/favicons/favicon-32x32.png">
      <link rel="icon" type="image/png" sizes="96x96" href="{{ site.baseurl }}/assets/images/favicons/favicon-96x96.png">
      <link rel="icon" type="image/png" sizes="16x16" href="{{ site.baseurl }}/assets/images/favicons/favicon-16x16.png">
      <link rel="manifest" href="{{ site.baseurl }}/assets/images/favicons/manifest.json">
      <meta name="msapplication-TileColor" content="#ffffff">
      <meta name="msapplication-TileImage" content="{{ site.baseurl }}/assets/images/favicons/ms-icon-144x144.png">
      <meta name="theme-color" content="#ffffff">

```

be really careful about the paths here. Too many broken links and it starts to affect your google ranking. 
