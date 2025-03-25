load("@aspect_bazel_lib//lib:expand_template.bzl", "expand_template")
load("@bazel_skylib//lib:paths.bzl", "paths")
load("@rules_multirun//:defs.bzl", "command", "multirun")
load("@rules_oci//oci:defs.bzl", "oci_image", "oci_image_index", "oci_push")
load("@tar.bzl", "mutate", "tar")

def sisyphus_pushable(
    name,
    binary_image,
    binary_repository,
    config_entrypoint,
    remote_tags,
    deps = [],
    platforms = None,
    srcs = [],
):
    # Make the binary
    binary_multiarch = name + "_binary_multiarch"
    oci_image_index(
        name = binary_multiarch,
        images = [binary_image],
        platforms = platforms or [
            "@dev_april_sisyphus//:linux_amd64",
            "@dev_april_sisyphus//:linux_arm64",
        ],
    )

    oci_push(
        name = name + "_binary_push",
        image = ":" + binary_multiarch,
        repository = binary_repository,
        remote_tags = ["latest"],
    )

    # Make the config
    digest = binary_image + ".digest"
    json_base = name + "_config_index_base_json"
    json_final = name + "_config_index_final_json"

    native.genrule(
        name = json_final,
        outs = ["index.json"],
        srcs = [
            ":" + json_base,
            digest,
        ],
        cmd = " ".join([
            "sed \"s/{DIGEST}/$$(cat $(execpath " + digest + "))/\" $(execpath :" + json_base + ")",
            " > \"$@\"",
        ]),
    )

    expand_template(
        name = json_base,
        out = name + "_index_base.json",
        template = [
          "{",
          "  \"binary_digest\": \"{DIGEST}\",",
          "  \"binary_image\": \"{BINARY_IMAGE}\",",
          "  \"config_entrypoint\": \"{CONFIG_ENTRYPOINT}\"",
          "}\n",
        ],
        substitutions = {
            "{BINARY_IMAGE}": binary_repository,
            "{CONFIG_ENTRYPOINT}": paths.join(native.package_name(), config_entrypoint),
        },
    )

    config_files_tar = name + "_config_files_tar"
    tar(
        name = config_files_tar,
        compress = "gzip",
        srcs = srcs + deps + [
            config_entrypoint,
        ],
    )

    config_index_tar = name + "_config_index_tar"
    tar(
        name = config_index_tar,
        compress = "gzip",
        srcs = [":" + json_final],
        mutate = mutate(strip_prefix = native.package_name()),
    )

    config_image = name + "_config_image"
    oci_image(
        name = config_image,
        architecture = "arm64",
        os = "linux",
        tars = [
            config_files_tar,
            config_index_tar,
        ],
    )

    config_multiarch = name + "_config_multiarch"
    oci_image_index(
        name = config_multiarch,
        images = [config_image],
        platforms = platforms or [
            "@dev_april_sisyphus//:linux_amd64",
            "@dev_april_sisyphus//:linux_arm64",
        ],
    )

    oci_push(
        name = name + "_config_push",
        image = ":" + config_multiarch,
        repository = binary_repository + "_config",
        remote_tags = remote_tags,
    )

    multirun(
        name = name + "_push",
        commands = [
            name + "_binary_push",
            name + "_config_push",
        ],
        jobs = 0,  # parallel
    )
