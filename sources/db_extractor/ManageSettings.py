

class ManageSettings:

    @staticmethod
    def fn_set_extract_behaviour(in_session):
        default_value = 'skip-if-output-file-exists'
        allowed_values = [
            'skip-if-output-file-exists',
            'overwrite-if-output-file-exists',
        ]
        if ('extract-behaviour' not in in_session) \
                or (in_session['extract-behaviour'] not in allowed_values):
            value_to_return = default_value
        else:
            value_to_return = in_session['extract-behaviour']
        return value_to_return
