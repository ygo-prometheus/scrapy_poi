from user_agent import generate_user_agent

class RandomUserAgentMiddleware(object):

    def process_request(self, request, spider):
        request.headers["User-Agent"] = generate_user_agent()