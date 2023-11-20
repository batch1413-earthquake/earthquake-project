deploy :
	docker build -t=europe-docker.pkg.dev/batch1413-earthquake/pubsub-app/app:0.1 .
	docker push europe-docker.pkg.dev/batch1413-earthquake/pubsub-app/app:0.1
	gcloud run deploy pubsub --image europe-docker.pkg.dev/batch1413-earthquake/pubsub-app/app:0.1 --platform managed
