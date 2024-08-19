function sendFormattedEmail() {
    var sheetNames = ['Summary_EmailFormat'];
    var ranges = ['A3:H16', 'A18:H38'];
    var sheet = SpreadsheetApp.getActiveSpreadsheet();
    var currentDate = Utilities.formatDate(new Date(), sheet.getSpreadsheetTimeZone(), 'dd MMM yyyy');

    // Get the HTML tables
    var Summary_EmailFormat_1 = getHtmlTable(sheet.getSheetByName(sheetNames[0]).getRange(ranges[0]));
    var Summary_EmailFormat_2 = getHtmlTable(sheet.getSheetByName(sheetNames[0]).getRange(ranges[1]));

    // Email details
    var emailAddress = [
        'puneet.pushkar@voltmoney.in'
    ]
    // 'lalit@voltmoney.in', 'product.ops@voltmoney.in', 
    // 'ranjan.singh@voltmoney.in', 'vaibhav.arora@voltmoney.in', 'ankit@voltmoney.in', 'saksham.srivastava@voltmoney.in', 
    // 'bharat@voltmoney.in', 'nitin.ghosh@voltmoney.in', 'tushar.luthra@voltmoney.in', 'akash.thakur@voltmoney.in'];

    var subject = 'Mandate Issues - ' + currentDate;
    var body = '<b>Success Rate:</b><br>' + Summary_EmailFormat_1 + Summary_EmailFormat_2 +
        '<br>For complete view, refer to sheet: <a href="https://docs.google.com/spreadsheets/d/152hB4pg1Rr0DfsJuKfRgreMwEg1roubHRrjCq-qHGus/edit?usp=sharing"> {Mandate issue}</a> <br><br> Regards<br>Puneet Pushkar';

    // Send email
    GmailApp.sendEmail(emailAddress, subject, '', { htmlBody: body });
}

function getHtmlTable(range, sheet) {
    var data = range.getDisplayValues();
    var numRows = data.length;
    var numCols = data[0].length;
    var html = '<table border="1" style="border-collapse: collapse; text-align: center; width: auto;">'; // Set table width to auto

    // Track merged cells to avoid repeating them in the HTML table
    var mergedCells = {};

    for (var i = 0; i < numRows; i++) {
        var row = range.getCell(i + 1, 1).getRow();
        html += '<tr>';
        for (var j = 0; j < numCols; j++) {
            var col = range.getCell(1, j + 1).getColumn();
            var cell = range.getCell(i + 1, j + 1);
            var cellValue = data[i][j];

            var backgroundColor = cell.getBackground();
            var fontColor = cell.getFontColor();
            var fontSize = cell.getFontSize() + 2;
            var fontWeight = cell.getFontWeight();
            var horizontalAlignment = cell.getHorizontalAlignment();
            var verticalAlignment = cell.getVerticalAlignment();
            var isMerged = cell.isPartOfMerge();
            var colspan = 1;
            var rowspan = 1;

            if (isMerged) {
                var mergeRange = cell.getMergedRanges()[0];
                var mergeRangeA1 = mergeRange.getA1Notation();
                if (!mergedCells[mergeRangeA1]) {
                    mergedCells[mergeRangeA1] = true;
                    colspan = mergeRange.getWidth();
                    rowspan = mergeRange.getHeight();
                } else {
                    continue;
                }
            }

            // Set horizontal padding to 10px and vertical padding to 4px
            var borderStyle = cellValue === '' ? 'border: none;' : '';
            // html += '<td style="background-color: ' + backgroundColor + '; color: ' + fontColor + '; font-size: ' + fontSize + 'px; font-weight: ' + fontWeight + '; text-align: ' + horizontalAlignment + '; vertical-align: ' + verticalAlignment + '; padding: 4px 10px; ' + borderStyle + '" colspan="' + colspan + '" rowspan="' + rowspan + '">' + cellValue + '</td>';
            html += '<td style="background-color: ' + backgroundColor + '; color: ' + fontColor + '; font-size: ' + fontSize + 'px; font-weight: ' + fontWeight + '; text-align: ' + horizontalAlignment + '; vertical-align: ' + verticalAlignment + '; padding: 4px 10px; ' + borderStyle + ' width: 100px;" colspan="' + colspan + '" rowspan="' + rowspan + '">' + cellValue + '</td>';
        }
        html += '</tr>';
    }
    html += '</table>';
    return html;
}
