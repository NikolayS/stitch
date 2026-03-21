# demos/demo2_analyze.tape — VHS tape for sqlever analyze demo
#
# Renders a GIF showing: analysis of a dangerous SQL migration file.
#
# Prerequisites:
#   - Install VHS: https://github.com/charmbracelet/vhs
#   - Have sqlever built or on PATH
#
# To render:
#   vhs demos/demo2_analyze.tape
#
# Output: demos/demo2_analyze.gif

Output demos/demo2_analyze.gif

Set Shell "bash"
Set FontSize 14
Set Width 1000
Set Height 600
Set Theme "Dracula"
Set Padding 20

# Title
Type "# sqlever — static analysis demo"
Enter
Sleep 1s

# Create a dangerous migration file
Type "cat > /tmp/dangerous.sql << 'SQL'"
Enter
Type "-- A migration with several dangerous patterns"
Enter
Type ""
Enter
Type "ALTER TABLE orders ADD COLUMN total numeric NOT NULL;"
Enter
Type ""
Enter
Type "CREATE INDEX idx_orders_total ON orders (total);"
Enter
Type ""
Enter
Type "ALTER TABLE orders ADD CONSTRAINT orders_total_positive"
Enter
Type "  CHECK (total >= 0);"
Enter
Type ""
Enter
Type "UPDATE orders SET total = 0;"
Enter
Type "SQL"
Enter
Sleep 1s

# Analyze the file
Type "sqlever analyze /tmp/dangerous.sql"
Enter
Sleep 4s

# Show JSON output too
Type "sqlever analyze /tmp/dangerous.sql --format json | head -30"
Enter
Sleep 3s

# Show suppression example
Type "echo '-- sqlever:disable SA010' | cat - /tmp/dangerous.sql"
Enter
Sleep 2s

Type "# 4 findings: SA001 (NOT NULL no default), SA004 (index not concurrent),"
Enter
Type "#   SA016 (CHECK not validated), SA010 (UPDATE without WHERE)"
Enter
Sleep 3s
