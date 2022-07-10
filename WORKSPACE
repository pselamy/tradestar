workspace(name = "tradestar")

load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository")

######################
#### JAVA SUPPORT ####
######################
git_repository(
    name = "contrib_rules_jvm",
    commit = "f7c08ec6d73ef691b03f843e0c2c3dbe766df584",
    remote = "https://github.com/bazel-contrib/rules_jvm",
    shallow_since = "1642674503 +0000",
)

load("@contrib_rules_jvm//:repositories.bzl", "contrib_rules_jvm_deps")

contrib_rules_jvm_deps()

load("@contrib_rules_jvm//:setup.bzl", "contrib_rules_jvm_setup")

contrib_rules_jvm_setup()

git_repository(
    name = "rules_jvm_external",
    remote = "https://github.com/bazelbuild/rules_jvm_external",
    tag = "4.2",
)

load("@rules_jvm_external//:defs.bzl", "maven_install")

maven_install(
    artifacts = [
        "com.google.auto.value:auto-value:1.9",
        "com.google.auto.value:auto-value-annotations:1.9",
        "com.google.guava:guava:31.1-jre",
        "com.google.inject:guice:5.1.0",
        "joda-time:joda-time:2.10.14",
        # Apache Beam
        "org.apache.beam:beam-sdks-java-core:2.39.0",
        "org.apache.beam:beam-sdks-java-extensions-protobuf:2.39.0",
        # Unit Test Dependencies
        "com.google.truth:truth:1.1.3",
    ],
    repositories = [
        "https://repo1.maven.org/maven2",
    ],
)

##########################
#### PROTOBUF SUPPORT ####
##########################
git_repository(
    name = "rules_proto",
    commit = "3212323502e21b819ac4fbdd455cb227ad0f6394",
    remote = "https://github.com/bazelbuild/rules_proto",
    shallow_since = "1649153521 +0200",
)

load("@rules_proto//proto:repositories.bzl", "rules_proto_dependencies", "rules_proto_toolchains")

rules_proto_dependencies()

rules_proto_toolchains()

########################
#### PYTHON SUPPORT ####
########################
git_repository(
    name = "rules_python",
    commit = "4c961d92a15f4a3f90faab82eecb18d91ee2ccbe",
    remote = "https://github.com/bazelbuild/rules_python",
    shallow_since = "1649153521 +0200",
)

load("@rules_python//python:repositories.bzl", "python_register_toolchains")

python_register_toolchains(
    name = "python3_9",
    # Available versions are listed in @rules_python//python:versions.bzl.
    # We recommend using the same version your team is already standardized on.
    python_version = "3.9",
)

load("@python3_9//:defs.bzl", "interpreter")
load("@rules_python//python:pip.bzl", "pip_parse")

pip_parse(
    name = "pip_deps",
    python_interpreter_target = interpreter,
    requirements_lock = "//:requirements_lock.txt",
)

# Load the starlark macro which will define your dependencies.
load("@pip_deps//:requirements.bzl", "install_deps")

# Call it to define repos for your requirements.
install_deps()

##########################
##### CORE LIBRARIES #####
##########################

git_repository(
    name = "tradestar_core",
    commit = "95d2c1a4ac4bf69469bb81dd085a16d7f39846fe",
    remote = "https://github.com/pselamy/tradestar-core",
    shallow_since = "1657475286 -0400",
)

git_repository(
    name = "tradestar_protos",
    commit = "99ab9fdef3ccf2fe1638de8f8110beb848e72afb",
    remote = "https://github.com/pselamy/tradestar-protos",
    shallow_since = "1657475286 -0400",
)
