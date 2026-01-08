var dagfuncs = window.dashAgGridFunctions = window.dashAgGridFunctions || {};

// Tree path: YYYY-MM -> Category -> Account -> Payee (leaf)
dagfuncs.getDataPath = function (data) {
  //return [data.run_month, data.category, data.account, data.payee];
  //return [data.category, data.run_month, data.account, data.payee];
  return [data.category, data.payee, data.run_month, data.txn_id];
};

// Tree path: Payee->YYYY-MM (leaf)
dagfuncs.getDataPathPayee = function (data) {
    //return [data.run_month, data.category, data.account, data.payee];
    //return [data.payee, data.category, data.run_month, data.txn_id];
    return [data.run_month, data.category, data.payee, data.txn_id];
};

dagfuncs.formatCents = function (cents) {
  if (cents == null) return '';
  return new Intl.NumberFormat('en-US', {
    useGrouping: true,
    minimumFractionDigits: 2,
    maximumFractionDigits: 2
  }).format(Number(cents) / 100);
};

// Right-align money; bold for group rows + pinned rows
dagfuncs.moneyCellStyle = function (params) {
  const style = {
      textAlign: 'right',
      fontFamily: 'monospace',
      fontSize: '16px',
  };
  if (params.node && (params.node.group || params.node.rowPinned)) {
    style.fontWeight = '700';
  }
  return style;
};

// Bold label in pinned row (e.g., "Grand Total")
dagfuncs.totalLabelStyle = function (params) {
  if (params.node && params.node.rowPinned) {
    return { fontWeight: '700' };
  }
  return null;
};