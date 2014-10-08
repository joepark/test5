### Directory: deeplinks-middle

AWS Elastic Beanstalk application.  Serves as an origin to AWS CloudFront.  Retrieves related deeplink search results given App id and unique article id.



### Directory: import_scripts

App specific data feed import scripts.  These are customized to specific app and include hard coded values.  To be run as cron job on backend.



### Directory: opsworks-elasticsearch-cookbook

It should be a separate repository.  Cookbook used in AWS opsworks to set up elasticsearch cluster.  Its Berksfile should point elasticsearch cookbook to custom cookbook-elasticsearch cookbook that is configured with v.1.3.1 of elasticsearch.



### Directory: cookbook-elasticsearch

It should be a seperate repository.  Modified to use elasticsearch version 1.3.1.  Path to this repository should be added in Berksfile in opsworks-elasticsearch-cookbook repository.

