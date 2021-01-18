range=200
number=$((RANDOM % range))
add_to_caption="Random Caption Repeated ${number} times"
caption=""
for ((run=1; run <= number; run++))
do
caption="${caption} ${add_to_caption}."
echo "$caption"
done
DOTENV_CONFIG_ENCODING=latin1 node -r dotenv/config add-post.js dotenv_config_path=./.env << ENDINPUT
c
$1
$2
$3
$caption
ENDINPUT

