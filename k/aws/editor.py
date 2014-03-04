import sys
import os
import os.path
import tempfile
from subprocess import call

def edit_temp_file(initial):
	editor = select_editor()
	with tempfile.NamedTemporaryFile(suffix=".tmp") as tf:
		tf.write(initial)
		tf.flush()
		call([editor, tf.name])
		tf.seek(0)
		content = tf.readlines()
		return ''.join(content)

def select_editor():
	"""
	Select editor using the following in order:

	$VISUAL
	$EDITOR
	~/.selected_editor
	/usr/bin/select-editor
	/usr/bin/vim
	/usr/bin/vi
	"""
	if os.getenv("VISUAL"):
		editor = os.path.expanduser(os.getenv("VISUAL"))
		editor_exists(editor, "$VISUAL")
		return editor
	if os.getenv("EDITOR"):
		editor = os.path.expanduser(os.getenv("EDITOR"))
		editor_exists(editor, "$EDITOR")
		return editor
	if os.path.exists(os.path.expanduser("~/.selected_editor")):
		editor = parse_selected_editor()
		if editor:
			editor_exists(editor, "~/.selected_editor")
			return editor
	if os.path.exists("/usr/bin/select-editor"):
		call(["/usr/bin/select-editor"])
		return select_editor()
	if os.path.exists("/usr/bin/vim"):
		return "/usr/bin/vim"
	if os.path.exists("/usr/bin/vi"):
		return "/usr/bin/vi"

def parse_selected_editor():
	with open(os.path.expanduser("~/.selected_editor")) as f:
		content = f.readlines()
		for line in content:
			if line.find("SELECTED_EDITOR=") > -1:
				editor = line.split("=")[1].strip().replace('"', '')
				if len(editor) > 0:
					return editor

def editor_exists(editor, method):
	if not os.path.exists(editor):
		sys.stderr.write("Selected editor (via %s) %s does not exist\n" % (
			method, editor))
		sys.exit(1)

USAGE = """
  if using -E, program will launch an editor attempting these in order:
    $VISUAL
    $EDITOR
    ~/.selected_editor
    /usr/bin/select-editor
    /usr/bin/vim
    /usr/bin/vi
"""

def editor_usage():
	return USAGE

def editor_options(parser, filename, delete=False):
	parser.add_option(
		"-r", dest="replace", default="False", action="store_true",
		help=("replace bucket's %s from STDIN" % filename))
	parser.add_option(
		"-E", dest="edit", default="False", action="store_true",
		help=("edit bucket's %s using EDITOR" % filename))
	parser.add_option(
		"-l", dest="list", default="False", action="store_true",
		help=("list bucket's %s" % filename))
	if delete:
		parser.add_option(
			"-d", dest="delete", default="False", action="store_true",
			help=("delete bucket's %s" % filename))

def run_editor(options, bucket, key, validation_function, help_text=None):
	if options.replace == True:
		if options.edit == True or options.list == True:
			sys.stderr.write("Only one of -r, -E or -l allowed.\n")
			sys.exit(1)
		replace_metadata_file(bucket, key, validation_function)
	elif options.edit == True:
		if options.list == True:
			sys.stderr.write("Only one of -r, -E or -l allowed\n")
			sys.exit(1)
		edit_metadata_file(bucket, key, validation_function, help_text)
	elif options.list == True:
		list_metadata_file(bucket, key)
	else:
		sys.stderr.write("One of -r, -E or -l required\n")
		sys.exit(1)

def get_metadata_file(bucket, key_name):
	key = bucket.get_key(key_name)
	if key:
		return key.get_contents_as_string()
	return ""

def put_metadata_file(bucket, key_name, contents):
	key = bucket.get_key(key_name)
	if not key:
		key = bucket.new_key(key_name)
	key.set_contents_from_string(contents)

def list_metadata_file(bucket, key_name):
	content = get_metadata_file(bucket, key_name)
	print content

def delete_metadata_file(bucket, key_name):
	bucket.delete_key(key_name)

def replace_metadata_file(bucket, key_name, validation_function):
	content = "".join(sys.stdin.readlines())
	if validation_function(content):
		put_metadata_file(bucket, key_name, content)

def edit_metadata_file(bucket, key_name, validation_function, help_text=None):
	content = get_metadata_file(bucket, key_name)
	comment = ""
	if help_text:
		comment = '\n'.join(["# " + line for line in help_text.split('\n')]).strip()
		if content.strip() == "":
			content = comment
	new_content = edit_temp_file(content)
	if new_content.strip() == comment:
		new_content = ""
	if validation_function(new_content):
		put_metadata_file(bucket, key_name, new_content)

