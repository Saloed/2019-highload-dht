profile_name="$1.yaml"
ammo_name="$1-ammo.gz"
docker run \
    -v $(pwd):/var/loadtest \
    -v $SSH_AUTH_SOCK:/ssh-agent -e SSH_AUTH_SOCK=/ssh-agent \
    --net host \
    -it direvius/yandex-tank \
    -c $profile_name $ammo_name


