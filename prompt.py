# Decorator to prompt for value for a constant.
# Apply to a function that is assumed to return a constant value.
# If the function returns None, then user will be prompted to supply value
# the first time the function is called. Subsequent calls will return that value.
def prompt( message ):
    # dummy class to hold prompt-scoped data
    class Prompt:
        pass
            
    Prompt.message = message   
    Prompt.cache = None

    def wrap( f ):
        if Prompt.message == '':
            Prompt.message = f.__name__

        def wrapped_f( *args ):
            if not Prompt.cache == None:
                return Prompt.cache
            
            val = f( *args )

            if val == None:
                val = raw_input( 'Provide value for ' + Prompt.message + ': ' )

            Prompt.cache = val
            
            return Prompt.cache
        return wrapped_f
    return wrap
