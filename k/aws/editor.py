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
