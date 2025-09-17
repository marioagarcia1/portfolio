SELECT * FROM (( [projeto_verx].[dbo].[backoffice]
	
	inner join financeiro on backoffice.ADE = financeiro.Adesão)
	inner join contratos on backoffice.ID = contratos.ID);
