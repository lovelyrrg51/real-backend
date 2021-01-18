#usage sh add_random_posts_to_real.sh number_of_posts_to_generate email password
#example usage sh add_random_posts_to_real.sh 10 example_email example_passwor
counter=0; until [ $counter -ge $1 ] ; do OUTPUT="$(python random_image.py 3840 2160 1)"; sh add_image_to_real_user.sh "$2" "$3" $OUTPUT; rm -r $OUTPUT;((counter++)); done
