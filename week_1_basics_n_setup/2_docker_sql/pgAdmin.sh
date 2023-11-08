docker run -it \
  -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
  -e PGADMIN_DEFAULT_PASSWORD="admin" \
  -p 8080:80 \
  dpage/pgadmin4
