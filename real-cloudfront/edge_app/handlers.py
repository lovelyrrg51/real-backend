import urllib


def viewer_request(event, context):
    """
    Handler to run on viewer_request events which:
      * authorizes the http method based on the Method querystirng parameter
      * authorized methods default to read-only methods (GET, HEAD) if not specified
    """
    # https://docs.aws.amazon.com/AmazonCloudFront/latest/DeveloperGuide/lambda-event-structure.html
    request = event['Records'][0]['cf']['request']
    http_method = request['method']
    parsed_qs = urllib.parse.parse_qs(request['querystring'])
    allowed_http_methods = parsed_qs.get('Method', ['GET', 'HEAD'])

    if http_method not in allowed_http_methods:
        return {'status': 403}

    # strip the querystring to avoid splitting the cloudfront cache by http method
    request['querystring'] = ''
    return request


def origin_request(event, context):
    """
    Handler to run on origin_request events which:
      * Adds the x-amz-acl header to give IAM users access to S3 objects.
        Without this, only the cloud front access identity user can access the
        objects and if that user gets deleted... then no more access to S3 objects.
    """
    request = event['Records'][0]['cf']['request']
    writes = ['PUT', 'POST', 'PATCH']
    if request['method'] in writes:
        request['headers']['x-amz-acl'] = [{'key': 'x-amz-acl', 'value': 'bucket-owner-full-control'}]
    return request
