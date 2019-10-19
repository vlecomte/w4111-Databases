def matches_template(row, template):
    result = True
    if template is not None:
        for k, v in template.items():
            if v != row.get(k, None):
                result = False
                break

    return result


def project(row, field_list):
    if field_list is None:
        return row
    res = {}
    for field in field_list:
        res[field] = row[field]
    return res
