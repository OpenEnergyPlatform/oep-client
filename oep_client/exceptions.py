class OepApiException(Exception):
    pass


class OepServerSideException(OepApiException):
    pass


class OepClientSideException(OepApiException):
    pass


class OepAuthenticationException(OepClientSideException):
    pass


class OepTableNotFoundException(OepClientSideException):
    def __init__(self, _msg=None):
        # the API falsely returns message: {'detail': 'You do not have permission to perform this action}
        # but this is only because table  does not exist
        # TODO: create better error message on server side!
        super().__init__(_msg or "Table does not exist OR you don't have permission")


class OepTableAlreadyExistsException(OepClientSideException):
    pass
