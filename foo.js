    const tableData = await rows.evaluateAll((rowElements) => {
        return rowElements.map(row => {
            return Array.from(row.querySelectorAll('td')).map(cell => cell.textContent?.trim());
        });
    })