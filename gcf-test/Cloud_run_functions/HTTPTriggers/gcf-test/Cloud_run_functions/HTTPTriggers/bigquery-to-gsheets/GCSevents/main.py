import functions_framework

@functions_framework.http
def square_digit(request):
    """HTTP Cloud Function to return the square of a digit"""
    request_json = request.get_json(silent=True)
    request_args = request.args

    if request_json and 'digit' in request_json:
        try:
            digit = int(request_json['digit'])
            return str(digit ** 2)
        except ValueError:
            return "Error: The 'digit' must be an integer.", 400
    elif request_args and 'digit' in request_args:
        try:
            digit = int(request_args['digit'])
            return str(digit ** 2)
        except ValueError:
            return "Error: The 'digit' must be an integer.", 400
    else:
        return "Error: Please provide a 'digit' parameter in the request.", 400


