# demos/demo1_deploy.tape — VHS tape for sqlever deploy demo
#
# Renders a GIF showing: init, add, deploy with TUI progress.
#
# Prerequisites:
#   - Install VHS: https://github.com/charmbracelet/vhs
#   - Have sqlever built or on PATH
#   - Have a running PostgreSQL instance at localhost:5432
#
# To render:
#   vhs demos/demo1_deploy.tape
#
# Output: demos/demo1_deploy.gif

Output demos/demo1_deploy.gif

Set Shell "bash"
Set FontSize 14
Set Width 1000
Set Height 600
Set Theme "Dracula"
Set Padding 20

# Title
Type "# sqlever — deploy demo"
Enter
Sleep 1s

# Initialize a project
Type "sqlever init myapp --engine pg"
Enter
Sleep 2s

# Add a migration
Type "sqlever add create_users -n 'Create users table'"
Enter
Sleep 2s

# Write deploy script
Type "cat > deploy/create_users.sql << 'SQL'"
Enter
Type "BEGIN;"
Enter
Type "CREATE TABLE users ("
Enter
Type "    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,"
Enter
Type "    email text NOT NULL UNIQUE,"
Enter
Type "    created_at timestamptz NOT NULL DEFAULT now()"
Enter
Type ");"
Enter
Type "COMMIT;"
Enter
Type "SQL"
Enter
Sleep 1s

# Analyze first
Type "sqlever analyze"
Enter
Sleep 3s

# Deploy (shows TUI progress bar)
Type "sqlever deploy db:pg://localhost/demo_sqlever"
Enter
Sleep 5s

# Status check
Type "sqlever status db:pg://localhost/demo_sqlever"
Enter
Sleep 3s

Type "# Done! Migration deployed with analysis."
Enter
Sleep 2s
