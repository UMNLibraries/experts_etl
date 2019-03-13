from jinja2 import Environment, PackageLoader, Template, select_autoescape
env = Environment(
    loader=PackageLoader('experts_etl', 'templates'),
    autoescape=select_autoescape(['html', 'xml'])
)

def serialize(org_dict):
    template = env.get_template('organisation.xml.j2')
    return template.render(org_dict)
