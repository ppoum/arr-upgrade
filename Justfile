sqlx-prepare:
        cargo sqlx prepare --workspace

build-image:
        buildah bud -f Dockerfile -t arr-upgrade --layers

run-image:
        @mkdir -p ./config-container
        podman run --name arr-upgrade-dev --rm -it -a stdout -a stderr -v ./config-container:/config localhost/arr-upgrade
