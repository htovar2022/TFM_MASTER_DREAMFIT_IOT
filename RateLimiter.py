class RateLimitManager:
    """
    Manages the rate limit information shared across multiple classes.
    """
    rate_limit = {
        'limit': 150,
        'remaining': 150,
        'reset': 0
    }

    @classmethod
    def update_rate_limit(cls, headers):
        """
        Updates the rate limit information from the response headers.

        Args:
            headers (requests.structures.CaseInsensitiveDict): The response headers from a Fitbit API request.
        """
        cls.rate_limit['limit'] = headers.get('fitbit-rate-limit-limit', cls.rate_limit['limit'])
        cls.rate_limit['remaining'] = headers.get('fitbit-rate-limit-remaining', cls.rate_limit['remaining'])
        cls.rate_limit['reset'] = headers.get('fitbit-rate-limit-reset', cls.rate_limit['reset'])