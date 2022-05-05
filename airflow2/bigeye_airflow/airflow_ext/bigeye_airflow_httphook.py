from airflow.providers.http.hooks.http import HttpHook


class CoatueHttpHook(HttpHook):
    """
    Supports Alternate forms of credentials.  Currently supports aws secretes.
    Overrides the default response_check with a custom one.
    :param response_check_func: a function that takes a Response (from the requests module)
        and raises an Exception if the response is not satisfactory. This Exception can be
        used to handle custom retry strategies in HttpOperators - eg. MosaicApiOperator
    :type response_check_func: function
    """
    def __init__(self,
                 response_check_func,
                 aws_secrets_id=None,
                 *args, **kwargs):
        super(CoatueHttpHook, self).__init__(
            *args, **kwargs
        )
        self.response_check_func = response_check_func
        self.aws_secrets_id = aws_secrets_id

    def check_response(self, response):
        """
        Overrides the default check_response function in HttpHook to
        use the custom func. if it exists.
        """
        if self.response_check_func:
            self.response_check_func(response)
        else:
            super().check_response(response)

    # override this to retrieve password from AWS secrets.
    def get_connection(self, conn_id):
        conn = super().get_connection(conn_id)
        if self.aws_secrets_id:
            password = get_secret_token(self.aws_secrets_id)
            conn.set_password(password)
        return conn