
Within the folder "metabase-k8s/infra:"

```bash
terraform init
```

```bash
terraform workspace new test
terraform workspace select test
```

replace metabase.tf with your metabase_sql_password, cluster_name and project_id :

```tf
metabase_sql_password="abc123"
cluster_name ="test"
project_id ="data-eng-wagon"
```


```bash
terraform apply -var-file="terraform.tvars"
```

update the dev/metabase.yaml file to replace with your project / cluster name / location

```yaml
#[...]
- name: cloudsql-proxy
          image: gcr.io/cloud-sql-connectors/cloud-sql-proxy:2.1.0
          args:
            - "--port=5433"
            - "PROJECT_ID:LOCATION:metabase-data-TERRAFORMWORKSPACE"
          resources:

#[...]
---

apiVersion: v1
kind: ServiceAccount
metadata:
  name: metabase-cloudsql-proxy
  annotations:
    iam.gke.io/gcp-service-account: metabase-service-account-TERRAFORMWORKSPAC@PROJECT_ID.iam.gserviceaccount.com
```


```bash
echo -n 'metabase-data-test' | base64   #dbname secret
echo -n 'metabase_user' | base64  #username secret

```

To monitor the pods, in particular the sql proxy
```bash
kubectl get pods
kubectl logs metabase-cloudsql-6f56bfcc45-rxnhf
kubectl logs metabase-cloudsql-6f56bfcc45-rxnhf -c cloudsql-proxy
```
