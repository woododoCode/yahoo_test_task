This is solution for test task

<h3>To test it:</h3>
<ol>
    <li>Clone repo</li>
    <li>Install all dependencies from _requirements.txt_</li>
    <li>Run app.py </li>
</ol>
<h3>Settings</h3>
In _config.py_ there are 2 fields:
<br>
MARKETS_LIST = ['PD', 'ZUO', 'PINS', 'ZM', 'PVTL', 'DOCU', 'CLDR', 'RUN']
<br>
DB_NAME = "markets.db.sqlite"
Change it if you need.

<b>To load data to DB visit `'/api/v1/load-data'` </b>
<b> Market data available in `'/api/v1/market/<string:market_name>'` from list of markets provided in _config.py_

P.S. No ORM was used, as I understand from task, as less frameworks is better =)