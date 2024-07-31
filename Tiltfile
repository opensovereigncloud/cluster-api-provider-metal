# -*- mode: Python -*-

envsubst_cmd = "./bin/envsubst"
kubectl_cmd = "./bin/kubectl"
kustomize_cmd = "./bin/kustomize"
helm_cmd = "./bin/helm"
tools_bin = "./bin"

#Add tools to path
os.putenv('PATH', os.getenv('PATH') + ':' + tools_bin)

update_settings(k8s_upsert_timeout_secs=60)  # on first tilt up, often can take longer than 30 seconds

# set defaults
settings = {
    "allowed_contexts": [
        "kind-capm"
    ],
    "preload_images_for_kind": True,
    "kind_cluster_name": "capm",
    "capi_version": "v1.6.4",
    "metal_version": "17e10339810f16b3e0261663620d36700b479710",
    "metal_image": "ghcr.io/ironcore-dev/metal-operator-controller-manager:sha-17e1033",
    "cert_manager_version": "v1.14.4",
    "kubernetes_version": "v1.29.4",
}

# global settings
settings.update(read_json(
    "tilt-settings.json",
    default = {},
))

if settings.get("trigger_mode") == "manual":
    trigger_mode(TRIGGER_MODE_MANUAL)

if "allowed_contexts" in settings:
    allow_k8s_contexts(settings.get("allowed_contexts"))

if "default_registry" in settings:
    default_registry(settings.get("default_registry"))

# deploy CAPI
def deploy_capi():
    version = settings.get("capi_version")
    capi_uri = "https://github.com/kubernetes-sigs/cluster-api/releases/download/{}/cluster-api-components.yaml".format(version)
    cmd = "curl -sSL {} | {} | {} apply -f -".format(capi_uri, envsubst_cmd, kubectl_cmd)
    local(cmd, quiet=True)
    if settings.get("extra_args"):
        extra_args = settings.get("extra_args")
        if extra_args.get("core"):
            core_extra_args = extra_args.get("core")
            if core_extra_args:
                for namespace in ["capi-system", "capi-webhook-system"]:
                    patch_args_with_extra_args(namespace, "capi-controller-manager", core_extra_args)
        if extra_args.get("kubeadm-bootstrap"):
            kb_extra_args = extra_args.get("kubeadm-bootstrap")
            if kb_extra_args:
                patch_args_with_extra_args("capi-kubeadm-bootstrap-system", "capi-kubeadm-bootstrap-controller-manager", kb_extra_args)

# deploy metal-operator
def deploy_metal():
    version = settings.get("metal_version")
    image = settings.get("metal_image")
    metal_uri = "https://github.com/ironcore-dev/metal-operator//config/default?ref={}".format(version)
    cmd = "{} build {} | {} | {} apply -f -".format(kustomize_cmd, metal_uri, envsubst_cmd, kubectl_cmd)
    local(cmd, quiet=True)

    patch_image("metal-operator-system", "metal-operator-controller-manager", image)

def patch_image(namespace, name, image):
    patch = [{
        "op": "replace",
        "path": "/spec/template/spec/containers/0/image",
        "value": image,
    }]
    local("kubectl patch deployment {} -n {} --type json -p='{}'".format(name, namespace, str(encode_json(patch)).replace("\n", "")))

def patch_metal_volume(namespace, name):
    patch = [{
        "op": "replace",
        "path": "/spec/template/spec/containers/0/image",
        "value": "foo",
    }]
    local("kubectl patch deployment {} -n {} --type json -p='{}'".format(name, namespace, str(encode_json(patch)).replace("\n", "")))

def patch_args_with_extra_args(namespace, name, extra_args):
    args_str = str(local('kubectl get deployments {} -n {} -o jsonpath={{.spec.template.spec.containers[1].args}}'.format(name, namespace)))
    args_to_add = [arg for arg in extra_args if arg not in args_str]
    if args_to_add:
        args = args_str[1:-1].split()
        args.extend(args_to_add)
        patch = [{
            "op": "replace",
            "path": "/spec/template/spec/containers/1/args",
            "value": args,
        }]
        local("kubectl patch deployment {} -n {} --type json -p='{}'".format(name, namespace, str(encode_json(patch)).replace("\n", "")))


# Users may define their own Tilt customizations in tilt.d. This directory is excluded from git and these files will
# not be checked in to version control.
def include_user_tilt_files():
    user_tiltfiles = listdir("tilt.d")
    for f in user_tiltfiles:
        include(f)


def append_arg_for_container_in_deployment(yaml_stream, name, namespace, contains_image_name, args):
    for item in yaml_stream:
        if item["kind"] == "Deployment" and item.get("metadata").get("name") == name and item.get("metadata").get("namespace") == namespace:
            containers = item.get("spec").get("template").get("spec").get("containers")
            for container in containers:
                if contains_image_name in container.get("image"):
                    container.get("args").extend(args)


def fixup_yaml_empty_arrays(yaml_str):
    yaml_str = yaml_str.replace("conditions: null", "conditions: []")
    return yaml_str.replace("storedVersions: null", "storedVersions: []")

tilt_helper_dockerfile_header = """
# Tilt image
FROM golang:1.22 as tilt-helper
# Support live reloading with Tilt
RUN wget --output-document /restart.sh --quiet https://raw.githubusercontent.com/windmilleng/rerun-process-wrapper/master/restart.sh  && \
    wget --output-document /start.sh --quiet https://raw.githubusercontent.com/windmilleng/rerun-process-wrapper/master/start.sh && \
    chmod +x /start.sh && chmod +x /restart.sh
"""

tilt_dockerfile_header = """
FROM gcr.io/distroless/base:debug as tilt
WORKDIR /
COPY --from=tilt-helper /start.sh .
COPY --from=tilt-helper /restart.sh .
COPY manager .
"""

# Build CAPM and add feature gates
def capm():
    # Apply the kustomized yaml for this provider
    substitutions = settings.get("kustomize_substitutions", {})
    os.environ.update(substitutions)

    yaml = str(kustomizesub("./config/default"))

    # add extra_args if they are defined
    if settings.get("extra_args"):
        extra_args = settings.get("extra_args").get("do")
        if extra_args:
            yaml_dict = decode_yaml_stream(yaml)
            append_arg_for_container_in_deployment(yaml_dict, "capm-controller-manager", "capm-system", "cluster-api-metal-controller", extra_args)
            yaml = str(encode_yaml_stream(yaml_dict))
            yaml = fixup_yaml_empty_arrays(yaml)

    # Set up a local_resource build of the provider's manager binary.
    local_resource(
        "manager",
        cmd = 'mkdir -p .tiltbuild;CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags \'-extldflags "-static"\' -o .tiltbuild/manager cmd/main.go',
        deps = ["api", "cmd", "config", "internal", "go.mod", "go.sum"]
    )

    dockerfile_contents = "\n".join([
        tilt_helper_dockerfile_header,
        tilt_dockerfile_header,
    ])

    entrypoint = ["sh", "/start.sh", "/manager"]
    extra_args = settings.get("extra_args")
    if extra_args:
        entrypoint.extend(extra_args)

    # Set up an image build for the provider. The live update configuration syncs the output from the local_resource
    # build into the container.
    docker_build(
        ref = "controller:latest",
        context = "./.tiltbuild/",
        dockerfile_contents = dockerfile_contents,
        target = "tilt",
        entrypoint = entrypoint,
        only = "manager",
        live_update = [
            sync(".tiltbuild/manager", "/manager"),
            run("sh /restart.sh"),
        ],
        ignore = ["templates"]
    )

    k8s_yaml(blob(yaml))

def deploy_cert_manager():
    version = settings.get("cert_manager_version")
    local('./bin/helm repo add jetstack https://charts.jetstack.io --force-update')
    local('./bin/helm upgrade --install cert-manager --namespace cert-manager --create-namespace jetstack/cert-manager --version v1.15.1 --set crds.enabled=true')

def base64_encode(to_encode):
    encode_blob = local("echo '{}' | tr -d '\n' | base64 - | tr -d '\n'".format(to_encode), quiet=True)
    return str(encode_blob)

def base64_encode_file(path_to_encode):
    encode_blob = local("cat {} | tr -d '\n' | base64 - | tr -d '\n'".format(path_to_encode), quiet=True)
    return str(encode_blob)

def read_file_from_path(path_to_read):
    str_blob = local("cat {} | tr -d '\n'".format(path_to_read), quiet=True)
    return str(str_blob)

def base64_decode(to_decode):
    decode_blob = local("echo '{}' | base64 --decode -".format(to_decode), quiet=True)
    return str(decode_blob)

def kustomizesub(folder):
    yaml = local('hack/kustomize-sub.sh {}'.format(folder), quiet=True)
    return yaml

def waitforsystem():
    local("kubectl wait --for=condition=ready --timeout=300s pod --all -n capi-kubeadm-bootstrap-system")
    local("kubectl wait --for=condition=ready --timeout=300s pod --all -n capi-kubeadm-control-plane-system")
    local("kubectl wait --for=condition=ready --timeout=300s pod --all -n capi-system")

##############################
# Actual work happens here
##############################

include_user_tilt_files()

deploy_cert_manager()

deploy_capi()

deploy_metal()

capm()

waitforsystem()


k8s_yaml('./templates/test/cluster_v1beta1_cluster.yaml')
k8s_resource(
    objects=['cluster-sample:cluster'],
    new_name='cluster-sample',
    trigger_mode=TRIGGER_MODE_MANUAL,
    auto_init=False
)

k8s_yaml('./config/samples/infrastructure_v1alpha1_metalcluster.yaml')
k8s_resource(
    objects=['metalcluster-sample:metalcluster'],
    new_name='metalcluster-sample',
    trigger_mode=TRIGGER_MODE_MANUAL,
    auto_init=False
)

k8s_yaml('./config/samples/infrastructure_v1alpha1_metalmachinetemplate.yaml')
k8s_resource(
    objects=['metalmachinetemplate-sample-control-plane:metalmachinetemplate'],
    new_name='metalmachinetemplate-sample-control-plane',
    trigger_mode=TRIGGER_MODE_MANUAL,
    auto_init=False
)

k8s_yaml('./templates/test/cluster_v1beta1_kubeadmcontrolplane.yaml')
k8s_resource(
    objects=['kubeadmcontrolplane-sample-cp:kubeadmcontrolplane'],
    new_name='kubeadmcontrolplane-sample-cp',
    trigger_mode=TRIGGER_MODE_MANUAL,
    auto_init=False
)
