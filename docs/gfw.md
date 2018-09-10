# Dealing with GFW blocking Dropbox.com download (instructions for "dev" mode)

DVA downloads model weights from Dropbox and Digial Ocean spaces, in some cases the files cannot be downloaded. As a workaround

1. Manually download files in trained_models.json [/configs/custom_defaults/trained_models.json](/configs/custom_defaults/trained_models.json).

2. Store downloaded files in new directory (models) inside the repo thus in dev mode that directory will appear inside the container at /root/DVA/models/.

3. Run `./dvactl configure`, set "dev" mode and set path of tranined models to "configs/custom_defaults/trained_models_local.json"

4. Run `./dvactl clean && ./dvactl start`.