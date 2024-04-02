
if [[ -z "$mitsuha_central_feed_token" ]]; then
  export mitsuha_central_feed_token="Bearer $(echo $(az account get-access-token --query "join(' ', ['Bearer', accessToken])" --output tsv) | cut -d ' ' -f2)"
fi

docker buildx build --push --platform linux/arm64 -t "$image_tag" --build-arg CARGO_REGISTRIES_MITSUHA_CENTRAL_FEED_TOKEN="$mitsuha_central_feed_token" .