ENVIRONMENT = "dev"

SRC_BUCKET_NAME = "crf-event"

FOLDERS = ["out_files"]

TARGET_PATH = r"\\GBWFNAS15AP.hbeu.adroot.hsbc\Input_AltApp2$\50_DBS\41_JLL_Outbound"
GSUTIL_EXECUTABLE = r"C:\swdtools\google-cloud-sdk-356.0.0-windows\bin\gsutil.cmd"
if ENVIRONMENT == "dev":
    GBQ_HTTP_PROXY = "http://googleapis-{}.gcp.cloud.uk.hsbc:3128".format(ENVIRONMENT)
    GBQ_HTTPS_PROXY = "http://googleapis-{}.gcp.cloud.uk.hsbc:3128".format(ENVIRONMENT)
else:
    GBQ_HTTP_PROXY = ""
    GBQ_HTTPS_PROXY = ""
#service_account = r"C:\Users\45282438\Documents\gcloud-key\gce-stage3\28-08-24\gce-stage3-image-builder-dev.json"
#service_account = r"C:\Users\45282438\Documents\gcloud-key\dbsrefinery-dev-svc-account\05-01-24\dbsrefinery-dev-svc-account.json"
service_account =r"C:\Users\45375853\Documents\cs_data_plotform\service_account\dbsr-pdtest-dev-svc-account.json"