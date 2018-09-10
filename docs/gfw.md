# Dealing with GFW blocking download

DVA downloads model weights from Dropbox and Digial Ocean spaces, in some cases the files cannot be downloaded. As a workaround

1. Manually download files in trained_models.json [/configs/custom_defaults/trained_models.json](/configs/custom_defaults/trained_models.json).

2. Store downloaded files in new directory (models) inside the repo thus in dev mode that directory will appear inside the container at /root/DVA/models/.

3. Run `./dvactl configure`, set "dev" mode and set path of tranined models to "configs/custom_defaults/trained_models_local.json"

4. Run `./dvactl clean && ./dvactl start`.

Note that these instructions are valid for "dev" mode since it relies on directory on host instance being
mapped as a shared docker volume with the webserver container. For Kube, CPU and GPU mode we recomment replacing the
urls with host that is not blocked in your environment.