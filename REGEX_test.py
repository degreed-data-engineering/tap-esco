from urllib.request import urlopen
import re

esco_url = "https://esco.ec.europa.eu/en/classification/skill_main"
start_string = '<div class="block-wrapper--esco_version">'
end_string = "</div>"
regex = "v\d\.\d\.\d"
html = urlopen(esco_url).read().decode("utf-8")
start_index = html.find(start_string) + len(start_string)
end_index = start_index + html[start_index:].find(end_string)
html_slice = html[start_index:end_index]
version = re.findall(regex, html_slice)
