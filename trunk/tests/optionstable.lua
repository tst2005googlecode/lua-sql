ud = {}    -- userdata (simulação)
ud.ac = 1  -- variavel do userdata (simulação)

ud.getautocommit = function( self )  -- metodo para recuperar o valor
	print( 'getautocommit', self, self.ac )
	return self.ac
end

ud.setautocommit = function( self, value )  -- metodo para alterar o valor
	print( 'setautocommit', self, value )
	self.ac = value
end

mtud = {}  -- metatable do userdata

mtud.__index = function( self, key )
	print( 'index(MTUD):', self, key )
	if key == 'options' then
		local opts, mto = {}, {}
		print( 'opts', opts, 'mto', mto )
		mto.__index = function( self, key )
			print( 'mtoi:', self, key )
			if key == 'autocommit' then
				return ud:getautocommit()
			end
		end

		mto.__newindex = function( self, key, value )
			print( 'mtoni:', self, key )
			if key == 'autocommit' then
				ud:setautocommit( value )
			end
		end

		mto.__metatable = 'cannot be assigned' 
		return setmetatable( opts, mto )
	end
end

mtud.__newindex = function( self, key )
	print( 'newindex(MTUD):', self, key )
	if key == 'options' then
		return
	end
end

setmetatable( ud, mtud )
mtud.__metatable = 'cannot be assigned' 

print( 'ud    ', ud, 'mtud', mtud, getmetatable( ud ) )
print( 'ud.options', ud.options )
print('====================================')
print( 'ud.o.ac', ud.options.autocommit )
print('----------------------------------------------')
print( 'ud.ac', ud.ac )

print('----------------------------------------------')
ud.options.autocommit = 0
print('----------------------------------------------')

print( 'ud.o.ac', ud.options.autocommit )
print('----------------------------------------------')
print( 'ud.ac', ud.ac )

ud.options = 'babaca'
print( 'options', ud.options )

--[[
env.options.locktimeout = n
con.options.autocommit = 1, 0
cur.options.modestring = 'a', 'n', 'an'
]]